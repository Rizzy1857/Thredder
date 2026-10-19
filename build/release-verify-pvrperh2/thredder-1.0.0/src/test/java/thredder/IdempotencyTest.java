package thredder;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

final class IdempotencyTest {
    private IdempotencyTest() { }
    static void check(boolean value, String message) { ConcurrencyTest.check(value, message); }
    static final IdempotentReceiver.Request REQUEST = new IdempotentReceiver.Request("logical-transfer", "A", "B", 100);
    static void run() throws Exception {
        Path dir = Files.createTempDirectory(Path.of("build"), "idempotency-test-");
        concurrentDuplicates(dir.resolve("duplicates"));
        rollbackAndAck(dir);
        crashes(dir);
        System.out.println("PASS: v0.5 duplicate/conflicting requests, rollback, lost acknowledgment, retention, corruption and receiver crash recovery");
    }
    static void concurrentDuplicates(Path path) throws Exception {
        try (IdempotentReceiver receiver = new IdempotentReceiver(path)) {
            var pool = Executors.newFixedThreadPool(8);
            CountDownLatch start = new CountDownLatch(1);
            try {
                List<Future<IdempotentReceiver.Result>> results = new ArrayList<>();
                for (int i = 0; i < 100; i++) results.add(pool.submit(() -> { ConcurrencyTest.await(start); return receiver.transfer(REQUEST); }));
                start.countDown();
                for (var future : results) check(future.get(3, TimeUnit.SECONDS).equals(new IdempotentReceiver.Result(REQUEST.key(), 9900, 10100)), "duplicates return one saved result");
                check(receiver.balance("A") == 9900 && receiver.balance("B") == 10100, "single business effect");
                try { receiver.transfer(new IdempotentReceiver.Request(REQUEST.key(), "A", "B", 101)); throw new AssertionError("conflicting key accepted"); }
                catch (IllegalArgumentException expected) { /* key uniqueness includes exact payload */ }
                receiver.transfer(new IdempotentReceiver.Request("second-key", "B", "A", 50));
                check(receiver.transfer(REQUEST).balanceA() == 9900 && receiver.balance("A") == 9950, "retry returns original result not current balances");
                boolean locked = false;
                try (IdempotentReceiver other = new IdempotentReceiver(path)) { other.balance("A"); }
                catch (RuntimeException expected) { locked = true; }
                check(locked, "exclusive receiver ownership");
            } finally { start.countDown(); pool.shutdownNow(); check(pool.awaitTermination(3, TimeUnit.SECONDS), "producer termination"); }
        }
        try (IdempotentReceiver receiver = new IdempotentReceiver(path)) {
            check(receiver.transfer(REQUEST).balanceA() == 9900 && receiver.balance("A") == 9950, "dedup survives restart");
            try { receiver.transfer(new IdempotentReceiver.Request("insufficient", "A", "B", 20_000)); throw new AssertionError("insufficient accepted"); }
            catch (IllegalArgumentException expected) { /* no effect and no completion */ }
            check(receiver.lookup("insufficient").isEmpty() && receiver.balance("A") == 9950, "validation rollback");
        }
        byte[] valid = Files.readAllBytes(path), damaged = valid.clone(); damaged[10] ^= 1;
        Path bad = Path.of(path + ".bad"); Files.write(bad, damaged);
        boolean rejected = false;
        try (IdempotentReceiver receiver = new IdempotentReceiver(bad)) { receiver.balance("A"); }
        catch (RuntimeException expected) { rejected = true; }
        check(rejected && Arrays.equals(Files.readAllBytes(bad), damaged), "corrupt receiver fails closed");
    }
    static void rollbackAndAck(Path dir) {
        for (String stage : List.of("before-replace", "after-replace", "after-commit-before-response")) {
            Path path = dir.resolve(stage);
            AtomicBoolean armed = new AtomicBoolean();
            try (IdempotentReceiver receiver = new IdempotentReceiver(path, boundary -> {
                if (armed.get() && stage.equals(boundary)) throw new IllegalStateException("Injected lost/failing response");
            })) {
                armed.set(true);
                try { receiver.transfer(REQUEST); throw new AssertionError("fault missing"); }
                catch (IllegalStateException expected) { /* must reconcile rather than infer failure */ }
                try { receiver.balance("A"); throw new AssertionError("uncertain receiver still available"); }
                catch (IllegalStateException expected) { /* poisoned */ }
            }
            try (IdempotentReceiver receiver = new IdempotentReceiver(path)) {
                boolean before = stage.equals("before-replace");
                check(receiver.balance("A") == (before ? 10000 : 9900), "effect matches commit boundary");
                check(receiver.lookup(REQUEST.key()).isPresent() != before, "completion and effect commit together");
                check(receiver.transfer(REQUEST).balanceA() == 9900 && receiver.balance("A") == 9900, "same-key retry safe after ambiguous failure");
            }
        }
    }
    static void crashes(Path dir) throws Exception {
        for (String stage : List.of("before-replace", "after-replace", "after-commit-before-response")) {
            Path path = dir.resolve("crash-" + stage);
            Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), "thredder.ReceiverCrashFixture", path.toString(), stage)
                .redirectErrorStream(true).redirectOutput(dir.resolve(stage + ".output").toFile()).start();
            try { check(child.waitFor(5, TimeUnit.SECONDS) && child.exitValue() == 72, "child halted at receiver commit boundary"); }
            finally { if (child.isAlive()) { child.destroyForcibly(); child.waitFor(3, TimeUnit.SECONDS); } }
            try (IdempotentReceiver receiver = new IdempotentReceiver(path)) {
                var request = new IdempotentReceiver.Request("crash-key", "A", "B", 100);
                boolean before = stage.equals("before-replace");
                check(receiver.lookup(request.key()).isPresent() != before && receiver.balance("A") == (before ? 10000 : 9900), "crash restores whole old or new transaction");
                check(receiver.transfer(request).balanceA() == 9900 && receiver.balance("A") == 9900, "crash retry one effect");
            }
        }
    }
}
