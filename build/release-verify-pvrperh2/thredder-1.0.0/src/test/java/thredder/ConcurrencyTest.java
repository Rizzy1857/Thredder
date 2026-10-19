package thredder;

import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;

/** Unsafe algorithms are isolated here, never used by the scheduler. */
final class ConcurrencyTest {
    private ConcurrencyTest() { }
    static void run() throws Exception {
        claims();
        deadlockFixture();
        resourceLocks();
        priorities();
        priorityCli();
        agingWithArrivals();
        TransferDemo.main(new String[0]);
        System.out.println("PASS: v0.2 claims, deadlock reproduction, lock cleanup, priorities, aging and transfers");
    }
    static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }
    static void await(CountDownLatch latch) throws InterruptedException {
        check(latch.await(3, TimeUnit.SECONDS), "latch deadline");
    }
    static void claims() throws Exception {
        var pool = Executors.newFixedThreadPool(8);
        try {
            AtomicBoolean unsafe = new AtomicBoolean();
            CyclicBarrier bothChecked = new CyclicBarrier(2);
            Callable<Boolean> broken = () -> {
                boolean available = !unsafe.get();
                bothChecked.await(3, TimeUnit.SECONDS);
                if (available) unsafe.set(true); // Deliberately separate check and write.
                return available;
            };
            var a = pool.submit(broken);
            var b = pool.submit(broken);
            check(a.get(3, TimeUnit.SECONDS) && b.get(3, TimeUnit.SECONDS), "unsafe duplicate claim reproduced");
            AtomicClaim safe = new AtomicClaim();
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Boolean>> attempts = new ArrayList<>();
            for (int i = 0; i < 100; i++) attempts.add(pool.submit(() -> { await(start); return safe.tryClaim(); }));
            start.countDown();
            int winners = 0;
            for (var attempt : attempts) if (attempt.get(3, TimeUnit.SECONDS)) winners++;
            check(winners == 1 && !safe.tryClaim(), "one atomic owner; claim cannot reset");
        } finally { pool.shutdownNow(); check(pool.awaitTermination(3, TimeUnit.SECONDS), "claim threads stopped"); }
    }
    static void deadlockFixture() throws Exception {
        ReentrantLock a = new ReentrantLock(), b = new ReentrantLock();
        CountDownLatch held = new CountDownLatch(2), attempting = new CountDownLatch(2);
        AtomicInteger interrupted = new AtomicInteger();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread first = opposite(a, b, held, attempting, interrupted, error);
        Thread second = opposite(b, a, held, attempting, interrupted, error);
        first.start(); second.start();
        try {
            await(attempting);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (!(a.hasQueuedThread(second) && b.hasQueuedThread(first)) && System.nanoTime() < deadline) Thread.yield();
            check(a.hasQueuedThread(second) && b.hasQueuedThread(first), "actual opposite-lock wait cycle observed");
        } finally {
            first.interrupt(); second.interrupt();
            first.join(3000); second.join(3000);
        }
        check(!first.isAlive() && !second.isAlive() && error.get() == null, "fixture terminates and releases");
        check(interrupted.get() >= 1 && !a.isLocked() && !b.isLocked(), "interrupt breaks deadlock fixture");
    }
    static Thread opposite(ReentrantLock first, ReentrantLock second, CountDownLatch held,
                           CountDownLatch attempting, AtomicInteger interrupted, AtomicReference<Throwable> error) {
        return new Thread(() -> {
            first.lock();
            try {
                held.countDown(); await(held); attempting.countDown();
                second.lockInterruptibly();
                try { /* An interrupt breaks the fixture's wait cycle. */ }
                finally { second.unlock(); }
            } catch (InterruptedException expected) { interrupted.incrementAndGet(); }
            catch (Throwable failure) { error.set(failure); }
            finally { first.unlock(); }
        }, "unsafe-deadlock-fixture");
    }
    static void resourceLocks() throws Exception {
        ResourceLockManager manager = new ResourceLockManager();
        var pool = Executors.newFixedThreadPool(2);
        CountDownLatch held = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try { manager.withLocks(List.of("a", "z"), () -> { throw new AssertionError("cancelled action ran"); }); }
            catch (InterruptedException expected) { cancelled.set(true); }
            catch (Throwable failure) { error.set(failure); }
        });
        try {
            var holder = pool.submit(() -> {
                manager.withLocks(List.of("z"), () -> { held.countDown(); await(release); }); return null;
            });
            await(held); waiter.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (waiter.getState() != Thread.State.WAITING && System.nanoTime() < deadline) Thread.yield();
            check(waiter.getState() == Thread.State.WAITING, "waiter holds a and waits for z");
            waiter.interrupt(); waiter.join(3000);
            check(cancelled.get() && error.get() == null && !waiter.isAlive(), "interrupt releases partial acquisition");
            pool.submit(() -> { manager.withLocks(List.of("a", "a"), () -> {}); return null; }).get(3, TimeUnit.SECONDS);
            release.countDown(); holder.get(3, TimeUnit.SECONDS);
            try {
                manager.withLocks(List.of("z", "a", "a"), () -> { throw new IllegalStateException("expected"); });
                throw new AssertionError("exception missing");
            } catch (IllegalStateException expected) { check(expected.getMessage().equals("expected"), "original error retained"); }
            pool.submit(() -> { manager.withLocks(List.of("a", "z"), () -> {}); return null; }).get(3, TimeUnit.SECONDS);
            AtomicBoolean nestedRejected = new AtomicBoolean();
            manager.withLocks(List.of("z"), () -> {
                try { manager.withLocks(List.of("a"), () -> {}); }
                catch (IllegalStateException expected) { nestedRejected.set(true); }
            });
            check(nestedRejected.get(), "nested acquisition cannot break ordering");
            CountDownLatch executing = new CountDownLatch(1);
            Thread action = new Thread(() -> {
                try { manager.withLocks(List.of("a"), () -> { executing.countDown(); new CountDownLatch(1).await(); }); }
                catch (InterruptedException expected) { /* cooperative action cancelled */ }
                catch (Exception failure) { error.set(failure); }
            });
            action.start();
            try { await(executing); } finally { action.interrupt(); action.join(3000); }
            check(!action.isAlive() && error.get() == null, "running action interrupted");
            pool.submit(() -> { manager.withLocks(List.of("a"), () -> {}); return null; }).get(3, TimeUnit.SECONDS);
        } finally {
            release.countDown(); waiter.interrupt(); waiter.join(3000);
            pool.shutdownNow(); check(pool.awaitTermination(3, TimeUnit.SECONDS), "lock threads stopped");
        }
    }
    static void priorities() throws Exception {
        for (Scheduler.Policy policy : Scheduler.Policy.values()) {
            AtomicLong clock = new AtomicLong();
            Scheduler scheduler = new Scheduler(1, 3, policy, Duration.ofNanos(1), clock::get);
            CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
            StringBuilder order = new StringBuilder();
            try {
                scheduler.submit("block", () -> { started.countDown(); await(release); }); await(started);
                scheduler.submit("low", 0, () -> order.append('L'));
                scheduler.submit("high1", 10, () -> order.append('A'));
                scheduler.submit("high2", 10, () -> order.append('B'));
                check(!scheduler.submit("overflow", () -> {}).accepted() && scheduler.queuedCount() == 3, "capacity under both policies");
                release.countDown(); scheduler.close();
                check(order.toString().equals(policy == Scheduler.Policy.FIFO ? "LAB" : "ABL"), "priority selection and FIFO ties");
                check(scheduler.list().stream().allMatch(j -> j.state() == Scheduler.State.SUCCEEDED), "priority jobs succeed");
                try { scheduler.submit("invalid", 11, () -> {}); throw new AssertionError("invalid priority accepted"); }
                catch (IllegalArgumentException expected) { /* range checked */ }
            } finally { release.countDown(); scheduler.close(); }
        }
    }
    static void priorityCli() throws Exception {
        try (Scheduler scheduler = new Scheduler(1, 8)) {
            java.io.StringWriter text = new java.io.StringWriter();
            Main.run(new java.io.BufferedReader(new java.io.StringReader(
                "priority 10 sleep 0\npriority 3 fail priority failure\npriority 11 sleep 0\npriority 2\nlist\nexit\n")),
                new java.io.PrintWriter(text, true), scheduler);
            check(scheduler.list().size() == 2, "priority CLI rejects invalid submissions");
            check(scheduler.list().get(0).priority() == 10 && scheduler.list().get(1).priority() == 3,
                "priority CLI preserves values");
            check(text.toString().contains("priority=10") && text.toString().contains("ERROR"), "priority CLI output");
        }
        for (Duration interval : List.of(Duration.ZERO, Duration.ofNanos(-1))) {
            boolean rejected = false;
            try (Scheduler scheduler = new Scheduler(1, 1, Scheduler.Policy.PRIORITY_AGING, interval, System::nanoTime)) {
                scheduler.list();
            } catch (IllegalArgumentException expected) { rejected = true; }
            check(rejected, "invalid aging interval rejected before workers start");
        }
    }
    static void agingWithArrivals() throws Exception {
        AtomicLong clock = new AtomicLong();
        Scheduler scheduler = new Scheduler(1, 3, Scheduler.Policy.PRIORITY_AGING, Duration.ofNanos(1), clock::get);
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        CountDownLatch lowRan = new CountDownLatch(1);
        AtomicInteger arrivals = new AtomicInteger();
        AtomicBoolean rejected = new AtomicBoolean();
        AtomicInteger arrivalsWhenLowRan = new AtomicInteger(-1);
        AtomicReference<java.util.UUID> lowId = new AtomicReference<>();
        try {
            scheduler.submit("block", () -> { started.countDown(); await(release); }); await(started);
            lowId.set(scheduler.submit("old low", 0, () -> { arrivalsWhenLowRan.set(arrivals.get()); lowRan.countDown(); }).id());
            class Arrival implements Scheduler.Job {
                public void execute() {
                    int n = arrivals.incrementAndGet();
                    clock.incrementAndGet();
                    if (n < 15 && !scheduler.submit("new high", 10, this).accepted()) rejected.set(true);
                }
            }
            scheduler.submit("new high", 10, new Arrival());
            release.countDown(); await(lowRan);
            // Low runs at age 10, tying a fresh high and winning by submission order.
            // Keep admissions open until the bounded continuing-arrival chain ends.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (arrivals.get() < 15 && System.nanoTime() < deadline) Thread.yield();
            check(arrivals.get() == 15 && !rejected.get(), "continued arrivals complete");
            check(arrivalsWhenLowRan.get() == 10, "aged low wins before stream ends at exact tie boundary");
            check(scheduler.status(lowId.get()).orElseThrow().queueWaitNanos() == 10L, "queue timing uses monotonic admission/start boundary");
            scheduler.close();
            check(scheduler.list().stream().allMatch(j -> j.state() == Scheduler.State.SUCCEEDED), "aging jobs succeed");
        } finally { release.countDown(); scheduler.close(); }
    }
}
