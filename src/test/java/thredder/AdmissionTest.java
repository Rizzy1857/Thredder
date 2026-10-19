package thredder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

final class AdmissionTest {
    private AdmissionTest() { }
    static void check(boolean value, String message) { ConcurrencyTest.check(value, message); }
    static void await(CountDownLatch latch) throws InterruptedException { ConcurrencyTest.await(latch); }

    static final class Full implements AutoCloseable {
        final Scheduler scheduler;
        final CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        Full(Scheduler.Policy policy) throws Exception {
            scheduler = new Scheduler(1, 1, policy, Duration.ofSeconds(1), () -> 0, 1);
            scheduler.submit("running", () -> { started.countDown(); await(release); });
            await(started);
            check(scheduler.submit("queued", () -> {}).accepted(), "fill queue");
        }
        public void close() { release.countDown(); scheduler.close(); }
    }
    static void waiting(Scheduler scheduler, Thread caller) {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while ((scheduler.activeSubmitters() != 1 || caller.getState() != Thread.State.TIMED_WAITING)
                && System.nanoTime() < end) Thread.yield();
        check(scheduler.activeSubmitters() == 1 && caller.getState() == Thread.State.TIMED_WAITING, "caller waiting for capacity");
    }
    static void run() throws Exception {
        for (Scheduler.Policy policy : Scheduler.Policy.values()) {
            timeoutAndCap(policy);
            capacityWakeup(policy);
            interruption(policy);
            shutdownWakeup(policy);
            admissionRace(policy);
        }
        validation();
        System.out.println("PASS: v0.3 timed admission, submitter bounds, permit cleanup and close races under both policies");
    }
    static void timeoutAndCap(Scheduler.Policy policy) throws Exception {
        try (Full full = new Full(policy)) {
            AtomicReference<Scheduler.Submission> result = new AtomicReference<>();
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread caller = new Thread(() -> {
                try { result.set(full.scheduler.offer("timeout", 0, () -> {}, Duration.ofMillis(200))); }
                catch (Throwable failure) { error.set(failure); }
            });
            long start = System.nanoTime(); caller.start();
            try {
                waiting(full.scheduler, caller);
                check("Concurrent submitter limit reached".equals(full.scheduler.submit("excess", () -> {}).rejection()), "immediate submit also capped");
                check("Concurrent submitter limit reached".equals(full.scheduler.offer("excess timed", 0, () -> {}, Duration.ofSeconds(1)).rejection()), "timed excess rejects");
                caller.join(2000);
                check(!caller.isAlive() && error.get() == null, "timeout caller completed");
                check("Admission timed out".equals(result.get().rejection()), "explicit timeout");
                check(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(200), "no premature timeout");
                check(full.scheduler.activeSubmitters() == 0 && full.scheduler.list().size() == 2, "timeout leaves no permit or ghost job");
                check("Queue is full".equals(full.scheduler.offer("zero", 0, () -> {}, Duration.ZERO).rejection()), "zero timeout immediate");
            } finally { caller.interrupt(); caller.join(2000); }
        }
    }
    static void capacityWakeup(Scheduler.Policy policy) throws Exception {
        try (Full full = new Full(policy)) {
            FutureTask<Scheduler.Submission> task = new FutureTask<>(() -> full.scheduler.offer("woken", 5, () -> {}, Duration.ofSeconds(2)));
            Thread caller = new Thread(task); caller.start();
            try {
                waiting(full.scheduler, caller);
                full.release.countDown();
                var result = task.get(2, TimeUnit.SECONDS);
                check(result.accepted(), "queue release admits waiter");
                full.scheduler.close();
                check(full.scheduler.status(result.id()).orElseThrow().state() == Scheduler.State.SUCCEEDED, "accepted work executes");
                check(full.scheduler.activeSubmitters() == 0, "success releases permit");
            } finally { caller.interrupt(); caller.join(2000); }
        }
    }
    static void interruption(Scheduler.Policy policy) throws Exception {
        try (Full full = new Full(policy)) {
            AtomicBoolean interrupted = new AtomicBoolean();
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread caller = new Thread(() -> {
                try { full.scheduler.offer("interrupted", 0, () -> {}, Duration.ofSeconds(5)); }
                catch (InterruptedException expected) { interrupted.set(true); }
                catch (Throwable failure) { error.set(failure); }
            });
            caller.start();
            try {
                waiting(full.scheduler, caller); caller.interrupt(); caller.join(2000);
                check(!caller.isAlive() && interrupted.get() && error.get() == null, "waiting caller interrupted");
                check(full.scheduler.activeSubmitters() == 0 && full.scheduler.list().size() == 2, "interruption cleans up without job");
                check("Queue is full".equals(full.scheduler.submit("reuse", () -> {}).rejection()), "permit reusable after interruption");
            } finally { caller.interrupt(); caller.join(2000); }
        }
    }
    static void shutdownWakeup(Scheduler.Policy policy) throws Exception {
        try (Full full = new Full(policy)) {
            FutureTask<Scheduler.Submission> task = new FutureTask<>(() -> full.scheduler.offer("closing", 0, () -> {}, Duration.ofSeconds(5)));
            Thread caller = new Thread(task), closer = new Thread(full.scheduler::close);
            caller.start();
            try {
                waiting(full.scheduler, caller); closer.start();
                check("Scheduler is closed".equals(task.get(2, TimeUnit.SECONDS).rejection()), "shutdown wakes caller before jobs drain");
                check(closer.isAlive() && full.scheduler.activeSubmitters() == 0, "close still drains while admission permit released");
                check(full.scheduler.list().size() == 2, "closed offer not registered");
            } finally {
                full.release.countDown(); caller.interrupt(); caller.join(2000); closer.join(2000);
                check(!closer.isAlive(), "closer terminated");
            }
        }
    }
    static void admissionRace(Scheduler.Policy policy) throws Exception {
        Scheduler scheduler = new Scheduler(2, 2, policy, Duration.ofMillis(1), System::nanoTime, 4);
        var pool = Executors.newFixedThreadPool(12);
        CountDownLatch start = new CountDownLatch(1);
        AtomicIntegerArray executions = new AtomicIntegerArray(40);
        List<Future<Scheduler.Submission>> results = new ArrayList<>();
        try {
            for (int i = 0; i < 40; i++) {
                final int index = i;
                results.add(pool.submit(() -> {
                    await(start);
                    var result = scheduler.offer("race " + index, index % 11, () -> executions.incrementAndGet(index), Duration.ofMillis(100));
                    check(scheduler.activeSubmitters() <= 4 && scheduler.queuedCount() <= 2, "admission and queue bounds");
                    return result;
                }));
            }
            var closed = pool.submit(() -> { await(start); scheduler.close(); return null; });
            start.countDown();
            List<Scheduler.Submission> collected = new ArrayList<>();
            for (var result : results) collected.add(result.get(3, TimeUnit.SECONDS));
            closed.get(3, TimeUnit.SECONDS);
            int accepted = 0;
            for (int i = 0; i < collected.size(); i++) {
                var result = collected.get(i);
                check(executions.get(i) == (result.accepted() ? 1 : 0), "race: exactly accepted jobs executed in-process");
                if (result.accepted()) {
                    accepted++;
                    check(scheduler.status(result.id()).orElseThrow().state() == Scheduler.State.SUCCEEDED, "accepted job remains observable");
                }
            }
            check(scheduler.list().size() == accepted && scheduler.activeSubmitters() == 0, "race accounting and permits");
        } finally {
            start.countDown(); pool.shutdownNow();
            check(pool.awaitTermination(3, TimeUnit.SECONDS), "race callers stopped"); scheduler.close();
        }
    }
    static void validation() throws Exception {
        try (Scheduler scheduler = new Scheduler(1, 1)) {
            for (Duration timeout : List.of(Duration.ofNanos(-1), Duration.ofSeconds(Long.MAX_VALUE))) {
                try { scheduler.offer("invalid", 0, () -> {}, timeout); throw new AssertionError("bad duration accepted"); }
                catch (IllegalArgumentException expected) { /* validated before permit */ }
            }
            Thread.currentThread().interrupt();
            try { scheduler.offer("pre-interrupted", 0, () -> {}, Duration.ZERO); throw new AssertionError("interrupt ignored"); }
            catch (InterruptedException expected) { check(!Thread.currentThread().isInterrupted(), "standard interrupt clearing"); }
            finally { Thread.interrupted(); }
            check(scheduler.activeSubmitters() == 0 && scheduler.list().isEmpty(), "invalid calls do not register");
        }
        AtomicBoolean failClock = new AtomicBoolean(true);
        try (Scheduler scheduler = new Scheduler(1, 1, Scheduler.Policy.FIFO, Duration.ofSeconds(1), () -> {
            if (failClock.get()) throw new IllegalStateException("clock failed"); return 0;
        }, 1)) {
            try { scheduler.offer("clock", 0, () -> {}, Duration.ZERO); throw new AssertionError("failure missing"); }
            catch (IllegalStateException expected) { /* even unexpected exceptions release permit */ }
            check(scheduler.activeSubmitters() == 0, "exception releases permit");
            failClock.set(false);
            check(scheduler.offer("after error", 0, () -> {}, Duration.ZERO).accepted(), "permit reusable after exception");
        }
    }
}
