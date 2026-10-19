package thredder;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

final class RecoveryTest {
    private RecoveryTest() { }
    static void check(boolean value, String message) { ConcurrencyTest.check(value, message); }
    static void await(CountDownLatch latch) throws InterruptedException { ConcurrencyTest.await(latch); }
    static Scheduler open(Path store) {
        return new Scheduler(1, 4, Scheduler.Policy.FIFO, Duration.ofSeconds(1), System::nanoTime, 2, store);
    }
    static void run() throws Exception {
        cooperativeShutdown();
        stubbornShutdown();
        interruptedShutdown();
        Path dir = Files.createTempDirectory(Path.of("build"), "recovery-test-");
        durableShutdown(dir.resolve("durable.log"));
        ownershipAndValidation(dir.resolve("owned.log"));
        crashBoundaries(dir);
        corruptFiles(dir);
        recoveredCapacity(dir.resolve("capacity.log"));
        storageFailures(dir);
        System.out.println("PASS: v0.4 deadlines, cancellation, unresolved handlers, persistence, seven crash scenarios and corrupt-store rejection");
    }
    static void cooperativeShutdown() throws Exception {
        Scheduler scheduler = new Scheduler(1, 2);
        ResourceLockManager locks = new ResourceLockManager();
        CountDownLatch running = new CountDownLatch(1);
        AtomicBoolean released = new AtomicBoolean();
        var job = scheduler.submit("cooperate", () -> locks.withLocks(List.of("resource"), () -> {
            running.countDown();
            try { new CountDownLatch(1).await(); }
            finally { released.set(true); }
        }));
        try {
            await(running);
            var queued = scheduler.submit("never run", () -> { throw new AssertionError("queued job ran"); });
            long start = System.nanoTime();
            var report = scheduler.shutdown(Duration.ofMillis(30), Duration.ofSeconds(1));
            check(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(30), "drain interval honored");
            check(report.terminated() && report.unresolved().isEmpty() && report.pending().isEmpty(), "cooperative shutdown completes");
            check(released.get() && scheduler.status(job.id()).orElseThrow().state() == Scheduler.State.CANCELLED, "cancelled after actual exit");
            check(scheduler.status(queued.id()).orElseThrow().state() == Scheduler.State.CANCELLED, "ephemeral queued cancellation");
            locks.withLocks(List.of("resource"), () -> {});
            check(!scheduler.submit("late", () -> {}).accepted(), "shutdown rejects admissions");
            check(scheduler.shutdown(Duration.ZERO, Duration.ZERO).terminated(), "shutdown repeat safe");
        } finally { scheduler.shutdown(Duration.ZERO, Duration.ofSeconds(1)); }
    }
    static void stubbornShutdown() throws Exception {
        Scheduler scheduler = new Scheduler(1, 1);
        CountDownLatch running = new CountDownLatch(1), release = new CountDownLatch(1);
        var job = scheduler.submit("ignores interrupt", () -> {
            running.countDown();
            while (release.getCount() > 0) {
                try { release.await(); } catch (InterruptedException ignored) { /* deliberately uncooperative */ }
            }
        });
        try {
            await(running);
            long start = System.nanoTime();
            var report = scheduler.shutdown(Duration.ofMillis(10), Duration.ofMillis(20));
            check(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2), "shutdown returns with stubborn handler");
            check(!report.terminated() && report.unresolved().equals(List.of(job.id())), "uncooperative job reported unresolved");
            check(scheduler.status(job.id()).orElseThrow().state() == Scheduler.State.UNRESOLVED, "not falsely cancelled");
            check(!scheduler.shutdown(Duration.ZERO, Duration.ZERO).terminated(), "repeat doesn't fake completion");
        } finally {
            release.countDown();
            check(scheduler.shutdown(Duration.ofSeconds(1), Duration.ofSeconds(1)).terminated(), "eventual handler completion observed");
        }
        check(scheduler.status(job.id()).orElseThrow().state() == Scheduler.State.SUCCEEDED, "handler returned normally after cancellation request");
    }
    static void interruptedShutdown() throws Exception {
        Scheduler scheduler = new Scheduler(1, 1);
        CountDownLatch running = new CountDownLatch(1);
        scheduler.submit("interrupt caller", () -> { running.countDown(); new CountDownLatch(1).await(); });
        await(running);
        Thread.currentThread().interrupt();
        try {
            scheduler.shutdown(Duration.ofSeconds(10), Duration.ofSeconds(10));
            check(Thread.currentThread().isInterrupted(), "shutdown preserves caller interruption");
        } finally {
            Thread.interrupted();
            check(scheduler.shutdown(Duration.ofSeconds(1), Duration.ofSeconds(1)).terminated(), "interrupted shutdown still requested cancellation");
        }
    }
    static void waitRunning(Scheduler scheduler, UUID id) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (scheduler.status(id).orElseThrow().state() == Scheduler.State.QUEUED && System.nanoTime() < deadline) Thread.yield();
        check(scheduler.status(id).orElseThrow().state() == Scheduler.State.RUNNING, "job entered running state");
    }
    static void durableShutdown(Path path) throws Exception {
        Scheduler scheduler = open(path);
        UUID running = scheduler.offer("running durable", 0, new JobDescriptor("sleep", "60000"), Duration.ZERO).id();
        waitRunning(scheduler, running);
        UUID queued = scheduler.offer("queued durable", 0, new JobDescriptor("sleep", "0"), Duration.ZERO).id();
        var report = scheduler.shutdown(Duration.ZERO, Duration.ofSeconds(1));
        check(report.terminated() && report.pending().equals(List.of(queued)) && report.storageError() == null, "durable pending preserved");
        check(scheduler.status(running).orElseThrow().state() == Scheduler.State.CANCELLED, "durable cancellation committed");
        Scheduler recovered = open(path);
        check(recovered.shutdown(Duration.ofSeconds(1), Duration.ofSeconds(1)).terminated(), "recovered queued job executes");
        check(recovered.status(queued).orElseThrow().state() == Scheduler.State.SUCCEEDED, "same queued ID restored");
        check(recovered.status(running).orElseThrow().state() == Scheduler.State.CANCELLED, "cancelled stays cancelled");
        Scheduler again = open(path);
        again.close();
        check(again.list().size() == 2 && again.status(queued).orElseThrow().state() == Scheduler.State.SUCCEEDED, "completion survives repeated restart");
    }
    static void ownershipAndValidation(Path path) throws Exception {
        Scheduler scheduler = open(path);
        try {
            boolean rejected = false;
            try (Scheduler other = open(path)) { other.list(); }
            catch (RuntimeException expected) { rejected = true; }
            check(rejected, "exclusive journal ownership");
            try { scheduler.submit("closure", () -> {}); throw new AssertionError("durable closure accepted"); }
            catch (IllegalArgumentException expected) { /* descriptors only */ }
            check(scheduler.list().isEmpty(), "closure never registered");
            try { scheduler.offer("x".repeat(70_000), 0, new JobDescriptor("sleep", "0"), Duration.ZERO); throw new AssertionError("oversized record accepted"); }
            catch (IllegalArgumentException expected) { /* no journal bytes written */ }
            check(scheduler.offer("valid", 0, new JobDescriptor("fail", "expected"), Duration.ZERO).accepted(), "valid request after bad record");
        } finally { scheduler.close(); }
        try (Scheduler recovered = open(path)) {
            check(recovered.list().get(0).state() == Scheduler.State.FAILED, "failed result persisted");
            check(recovered.list().get(0).error().contains("expected"), "failure detail persisted");
        }
    }
    static void crashBoundaries(Path dir) throws Exception {
        String[] boundaries = {"before-write:QUEUED", "after-force:QUEUED", "before-write:RUNNING",
            "after-force:RUNNING", "before-write:SUCCEEDED", "after-force:SUCCEEDED", "torn-accept"};
        for (int i = 0; i < boundaries.length; i++) {
            Path path = dir.resolve("crash-" + i + ".log");
            Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), "thredder.CrashFixture", path.toString(), boundaries[i])
                .redirectErrorStream(true).redirectOutput(dir.resolve("child-" + i + ".txt").toFile()).start();
            try {
                check(child.waitFor(5, TimeUnit.SECONDS) && child.exitValue() == 71, "child crash at " + boundaries[i]);
            } finally { if (child.isAlive()) { child.destroyForcibly(); child.waitFor(3, TimeUnit.SECONDS); } }
            UUID id = UUID.fromString(Files.readString(Path.of(path + ".id")));
            if (i == 6) {
                byte[] before = Files.readAllBytes(path);
                boolean rejected = false;
                try (Scheduler invalid = open(path)) { invalid.list(); }
                catch (RuntimeException expected) { rejected = true; }
                check(rejected && Arrays.equals(before, Files.readAllBytes(path)), "torn process-crash record fails closed");
                continue;
            }
            Scheduler recovered = open(path);
            recovered.shutdown(Duration.ofSeconds(1), Duration.ofSeconds(1));
            if (i == 0) check(recovered.list().isEmpty(), "crash before acceptance creates no job");
            else if (i == 3 || i == 4) {
                var snapshot = recovered.status(id).orElseThrow();
                check(snapshot.state() == Scheduler.State.UNRESOLVED && snapshot.finishedAt() == null, "uncertain execution never blindly replayed");
                try (Scheduler again = open(path)) {
                    check(again.status(id).orElseThrow().state() == Scheduler.State.UNRESOLVED, "unresolved stable across restarts");
                }
            } else check(recovered.status(id).orElseThrow().state() == Scheduler.State.SUCCEEDED, "durable queue/completion recovered at " + boundaries[i]);
        }
    }
    static void corruptFiles(Path dir) throws Exception {
        byte[] valid = Files.readAllBytes(dir.resolve("crash-5.log"));
        for (int i = 0; i < 3; i++) {
            byte[] bad = i == 0 ? Arrays.copyOf(valid, valid.length - 1) : valid.clone();
            if (i == 1) bad[20] ^= 1;
            if (i == 2) bad[0] ^= 1;
            Path path = dir.resolve("corrupt-" + i + ".log"); Files.write(path, bad);
            boolean rejected = false;
            try (Scheduler scheduler = open(path)) { scheduler.list(); }
            catch (RuntimeException expected) { rejected = true; }
            check(rejected && Arrays.equals(bad, Files.readAllBytes(path)), "corrupt/torn/version data rejected without rewrite");
            // A failed open must release its lock too.
            try (Journal journal = new Journal(path, Journal.Probe.NONE)) { check(journal != null, "failed-open lock released"); }
        }
    }
    static void recoveredCapacity(Path path) {
        try (Journal journal = new Journal(path, Journal.Probe.NONE)) {
            for (int i = 0; i < 2; i++) journal.append(new Journal.Record(UUID.randomUUID(), "queued " + i,
                new JobDescriptor("sleep", "0"), 0, Scheduler.State.QUEUED, Instant.now(), null, null, null));
        }
        boolean rejected = false;
        try (Scheduler scheduler = new Scheduler(1, 1, Scheduler.Policy.FIFO, Duration.ofSeconds(1), System::nanoTime, 1, path)) { scheduler.list(); }
        catch (IllegalArgumentException expected) { rejected = true; }
        check(rejected, "restart capacity cannot silently drop jobs");
        Scheduler scheduler = open(path); scheduler.close();
        check(scheduler.list().size() == 2 && scheduler.list().stream().allMatch(j -> j.state() == Scheduler.State.SUCCEEDED), "retry with sufficient capacity");
    }
    static void storageFailures(Path dir) throws Exception {
        for (Scheduler.State failedState : List.of(Scheduler.State.QUEUED, Scheduler.State.RUNNING, Scheduler.State.SUCCEEDED)) {
            Path path = dir.resolve("fault-" + failedState + ".log");
            Journal.Probe probe = (stage, record) -> {
                if (stage.equals("before-write") && record.state() == failedState) throw new IllegalStateException("injected storage fault");
            };
            Scheduler scheduler = new Scheduler(1, 2, Scheduler.Policy.FIFO, Duration.ofSeconds(1), System::nanoTime, 2, path, probe);
            try { scheduler.offer("fault", 0, new JobDescriptor("sleep", "0"), Duration.ZERO); }
            catch (IllegalStateException expected) { check(failedState == Scheduler.State.QUEUED, "only acceptance fault propagates to caller"); }
            var report = scheduler.shutdown(Duration.ofSeconds(1), Duration.ofSeconds(1));
            check(report.storageError() != null && scheduler.activeSubmitters() == 0, "store fault visible and permits released");
            check(!scheduler.offer("late", 0, new JobDescriptor("sleep", "0"), Duration.ZERO).accepted(), "store fault closes admissions");
            if (failedState != Scheduler.State.QUEUED) check(scheduler.list().get(0).state() == Scheduler.State.UNRESOLVED, "uncommitted outcome not advertised as success");
        }
    }
}
