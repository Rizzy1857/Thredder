package thredder;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/** Dependency-free test runner; failures exit nonzero, whole suite has a watchdog. */
public final class SchedulerTest {
    private SchedulerTest() { }

    public static void main(String[] args) throws Exception {
        Thread watchdog = new Thread(() -> {
            try { Thread.sleep(45_000); }
            catch (InterruptedException done) { return; }
            System.err.println("FAIL: suite timed out");
            System.exit(1);
        }, "test-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();
        capacityLifecycleAndFifo();
        concurrentSubmissions();
        workerLimit();
        failureIsolation();
        shutdown();
        cli();
        invalidConfiguration();
        ConcurrencyTest.run();
        AdmissionTest.run();
        RecoveryTest.run();
        IdempotencyTest.run();
        check(Thread.getAllStackTraces().keySet().stream()
            .noneMatch(t -> t.isAlive() && t.getName().startsWith("thredder-worker-")), "worker leak");
        watchdog.interrupt();
        System.out.println("PASS: 7 MVP groups plus v0.2–v0.5 coverage; no remaining scheduler workers");
    }

    private static void capacityLifecycleAndFifo() throws Exception {
        Scheduler scheduler = new Scheduler(1, 2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        StringBuilder order = new StringBuilder();
        try {
            var first = scheduler.submit("first", () -> { started.countDown(); await(release); });
            await(started);
            var second = scheduler.submit("second", () -> order.append('2'));
            var third = scheduler.submit("third", () -> order.append('3'));
            check(first.accepted() && second.accepted() && third.accepted(), "accept within capacity");
            check(!scheduler.submit("overflow", () -> {}).accepted(), "reject full queue");
            check(scheduler.queuedCount() == 2, "bounded queue");
            check(scheduler.list().size() == 3, "rejected job not registered");
            var snapshot = scheduler.status(second.id()).orElseThrow();
            check(snapshot.state() == Scheduler.State.QUEUED && snapshot.startedAt() == null, "queued state");
            check(scheduler.status(first.id()).orElseThrow().state() == Scheduler.State.RUNNING, "running state");
            release.countDown();
            scheduler.close();
            check(order.toString().equals("23"), "FIFO dequeue order with one worker");
            check(snapshot.state() == Scheduler.State.QUEUED, "snapshot remains immutable");
            for (var job : scheduler.list()) {
                check(job.state() == Scheduler.State.SUCCEEDED, "success state");
                check(job.startedAt() != null && job.finishedAt() != null && job.error() == null, "completion metadata");
            }
            check(scheduler.status(UUID.randomUUID()).isEmpty(), "unknown ID");
        } finally { release.countDown(); scheduler.close(); }
    }

    private static void concurrentSubmissions() throws Exception {
        int count = 200;
        Scheduler scheduler = new Scheduler(4, count);
        var producers = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch submitted = new CountDownLatch(count);
        AtomicInteger executions = new AtomicInteger();
        AtomicIntegerArray perJob = new AtomicIntegerArray(count);
        AtomicInteger rejected = new AtomicInteger();
        try {
            for (int i = 0; i < count; i++) {
                final int index = i;
                producers.submit(() -> {
                    try {
                        await(start);
                        if (!scheduler.submit("concurrent", () -> {
                            perJob.incrementAndGet(index);
                            executions.incrementAndGet();
                        }).accepted()) rejected.incrementAndGet();
                        scheduler.list();
                    } catch (Exception ex) { rejected.incrementAndGet(); }
                    finally { submitted.countDown(); }
                });
            }
            start.countDown();
            await(submitted);
            scheduler.close();
            check(rejected.get() == 0 && executions.get() == count, "all submissions executed once");
            var ids = new HashSet<UUID>();
            for (var job : scheduler.list()) {
                check(ids.add(job.id()), "unique IDs");
                check(job.state() == Scheduler.State.SUCCEEDED, "concurrent completion");
            }
            check(ids.size() == count, "all jobs retained");
            for (int i = 0; i < count; i++) check(perJob.get(i) == 1, "each job executes once in-process");
        } finally {
            start.countDown();
            producers.shutdownNow();
            check(producers.awaitTermination(3, TimeUnit.SECONDS), "producer termination");
            scheduler.close();
        }
    }

    private static void workerLimit() throws Exception {
        Scheduler scheduler = new Scheduler(3, 20);
        CountDownLatch running = new CountDownLatch(3);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        AtomicInteger total = new AtomicInteger();
        try {
            for (int i = 0; i < 20; i++) {
                check(scheduler.submit("bounded", () -> {
                    int current = active.incrementAndGet();
                    maximum.accumulateAndGet(current, Math::max);
                    running.countDown();
                    try { await(release); total.incrementAndGet(); }
                    finally { active.decrementAndGet(); }
                }).accepted(), "worker-limit submission");
            }
            await(running);
            check(active.get() == 3, "three workers actually overlap");
            release.countDown();
            scheduler.close();
            check(maximum.get() == 3 && total.get() == 20, "fixed concurrency and full execution");
        } finally { release.countDown(); scheduler.close(); }
    }

    private static void failureIsolation() {
        Scheduler scheduler = new Scheduler(1, 8);
        var failed = scheduler.submit("failure", () -> { throw new IllegalStateException("expected failure"); });
        var interrupted = scheduler.submit("interruption", () -> { Thread.currentThread().interrupt(); });
        var next = scheduler.submit("next", () -> check(!Thread.currentThread().isInterrupted(), "interrupt isolation"));
        var error = scheduler.submit("assertion", () -> { throw new AssertionError("handler assertion"); });
        var finalJob = scheduler.submit("still alive", () -> {});
        scheduler.close();
        check(scheduler.status(failed.id()).orElseThrow().error().contains("expected failure"), "failure details");
        check(scheduler.status(failed.id()).orElseThrow().state() == Scheduler.State.FAILED, "failed state");
        check(scheduler.status(error.id()).orElseThrow().state() == Scheduler.State.FAILED, "handler error captured");
        for (var id : new UUID[]{interrupted.id(), next.id(), finalJob.id()}) {
            check(scheduler.status(id).orElseThrow().state() == Scheduler.State.SUCCEEDED, "worker survives failure");
        }
    }

    private static void shutdown() throws Exception {
        Scheduler scheduler = new Scheduler(1, 2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger completed = new AtomicInteger();
        scheduler.submit("running", () -> { started.countDown(); await(release); completed.incrementAndGet(); });
        await(started);
        scheduler.submit("queued", completed::incrementAndGet);
        Thread closer = new Thread(scheduler::close);
        try {
            closer.start();
            // Wait until close has acquired the admission lock, without assuming a timing delay.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            boolean closed = false;
            while (System.nanoTime() < deadline) {
                var result = scheduler.submit("racing", completed::incrementAndGet);
                if ("Scheduler is closed".equals(result.rejection())) { closed = true; break; }
                Thread.yield();
            }
            check(closed && closer.isAlive(), "close rejects admission while draining");
            release.countDown();
            closer.join(3_000);
            check(!closer.isAlive(), "close joins workers");
            check(completed.get() == scheduler.list().size(), "drain every accepted job including submission race");
            check(!scheduler.submit("late", () -> {}).accepted(), "post-close rejection");
            scheduler.close();
        } finally { release.countDown(); scheduler.close(); closer.join(3_000); }
    }

    private static void cli() throws Exception {
        Scheduler scheduler = new Scheduler(1, 8);
        try {
            StringWriter text = new StringWriter();
            String commands = "help\nlist\nsubmit sleep 0\nsubmit fail demo failure\nstatus invalid\n"
                + "status 00000000-0000-0000-0000-000000000000\nsubmit sleep -1\nsubmit unknown x\nlist extra\nwhat\nexit\n";
            Main.run(new BufferedReader(new StringReader(commands)), new PrintWriter(text, true), scheduler);
            scheduler.close();
            String output = text.toString();
            check(output.contains("No jobs") && output.contains("ACCEPTED "), "CLI submission and empty list");
            check(output.contains("NOT FOUND") && output.contains("ERROR") && output.contains("Draining"), "CLI errors and exit");
            check(scheduler.list().size() == 2, "invalid commands do not submit work");
            text.getBuffer().setLength(0);
            UUID id = scheduler.list().get(1).id();
            Main.run(new BufferedReader(new StringReader("status " + id + "\nlist\nsubmit sleep 0\n")),
                new PrintWriter(text, true), scheduler);
            check(text.toString().contains("FAILED") && text.toString().contains("demo failure"), "CLI status details");
            check(text.toString().contains("REJECTED") && text.toString().contains("End of input"), "CLI rejection and EOF");
        } finally { scheduler.close(); }
    }

    private static void invalidConfiguration() {
        for (int[] values : new int[][]{{0, 1}, {1, 0}, {-1, 2}}) {
            boolean rejected = false;
            try (Scheduler ignored = new Scheduler(values[0], values[1])) {
                check(ignored.list().isEmpty(), "empty registry");
            } catch (IllegalArgumentException expected) { rejected = true; }
            check(rejected, "invalid configuration rejected");
        }
        try (Scheduler scheduler = new Scheduler(1, 1)) {
            boolean rejected = false;
            try { scheduler.submit("", () -> {}); }
            catch (IllegalArgumentException expected) { rejected = true; }
            check(rejected, "blank job rejected");
        }
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        check(latch.await(3, TimeUnit.SECONDS), "latch deadline");
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
