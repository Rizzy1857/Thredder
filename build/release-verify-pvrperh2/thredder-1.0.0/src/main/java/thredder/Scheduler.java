package thredder;

import java.time.Instant;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Bounded scheduler with optional journal persistence. A job runs as one indivisible scheduling unit. */
public final class Scheduler implements AutoCloseable {
    public enum Policy { FIFO, PRIORITY_AGING }

    public enum State { QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED, UNRESOLVED }

    @FunctionalInterface
    public interface Job { void execute() throws Exception; }

    public record Snapshot(UUID id, String name, State state, Instant submittedAt,
                           Instant startedAt, Instant finishedAt, String error, int priority, Long queueWaitNanos) { }

    public record Submission(UUID id, String rejection) {
        public boolean accepted() { return id != null; }
    }

    public record ShutdownReport(boolean terminated, List<UUID> pending, List<UUID> unresolved, String storageError) { }

    private static final class Entry {
        private final UUID id;
        private final String name;
        private Job job;
        private final AtomicClaim claim = new AtomicClaim();
        private final int priority;
        private final long enqueuedAt;
        private final Instant submittedAt;
        private final JobDescriptor descriptor;
        private Long queueWaitNanos;
        private Thread runner;
        private boolean cancellationRequested;
        private State state = State.QUEUED;
        private Instant startedAt;
        private Instant finishedAt;
        private String error;

        Entry(String name, Job job, int priority, long enqueuedAt, JobDescriptor descriptor) {
            this.id = UUID.randomUUID(); this.submittedAt = Instant.now();
            this.name = name; this.job = job; this.priority = priority; this.enqueuedAt = enqueuedAt;
            this.descriptor = descriptor;
        }
        Entry(Journal.Record record, long now) {
            id = record.id(); name = record.name(); descriptor = record.descriptor();
            priority = record.priority(); enqueuedAt = now; submittedAt = record.submitted();
            startedAt = record.started(); finishedAt = record.finished(); error = record.error(); state = record.state();
            job = state == State.QUEUED ? descriptor.handler() : null;
        }
        Journal.Record record() {
            return new Journal.Record(id, name, descriptor, priority, state, submittedAt, startedAt, finishedAt, error);
        }
        Snapshot snapshot() {
            return new Snapshot(id, name, state, submittedAt, startedAt, finishedAt, error, priority, queueWaitNanos);
        }
    }

    // Registry and lifecycle transitions share one monitor. No user job runs under it.
    private final Object gate = new Object();
    private final Map<UUID, Entry> registry = new LinkedHashMap<>();
    private final List<Entry> queue = new ArrayList<>();
    private final int queueCapacity;
    private final Policy policy;
    private final long agingNanos;
    private final LongSupplier clock;
    private final List<Thread> workers = new ArrayList<>();
    private boolean accepting = true;
    private final Semaphore submitters;
    private final int maxSubmitters;
    private final Journal journal;
    private int liveWorkers;
    private boolean stopDispatch;
    private String storageError;
    private boolean journalClosed;

    public Scheduler(int workerCount, int queueCapacity) {
        this(workerCount, queueCapacity, Policy.FIFO, Duration.ofSeconds(1), System::nanoTime);
    }

    /** Clock must be monotonic; elapsed intervals must be shorter than 292 years. */
    public Scheduler(int workerCount, int queueCapacity, Policy policy,
                     Duration agingInterval, LongSupplier clock) {
        this(workerCount, queueCapacity, policy, agingInterval, clock, 16);
    }

    public Scheduler(int workerCount, int queueCapacity, Policy policy,
                     Duration agingInterval, LongSupplier clock, int maxSubmitters) {
        this(workerCount, queueCapacity, policy, agingInterval, clock, maxSubmitters, null);
    }

    public Scheduler(int workerCount, int queueCapacity, Policy policy,
                     Duration agingInterval, LongSupplier clock, int maxSubmitters, Path store) {
        this(workerCount, queueCapacity, policy, agingInterval, clock, maxSubmitters, store, Journal.Probe.NONE);
    }

    Scheduler(int workerCount, int queueCapacity, Policy policy,
              Duration agingInterval, LongSupplier clock, int maxSubmitters, Path store, Journal.Probe probe) {
        if (maxSubmitters < 1) throw new IllegalArgumentException("Submitter cap must be positive");
        this.maxSubmitters = maxSubmitters;
        this.submitters = new Semaphore(maxSubmitters);
        if (workerCount < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("Workers and queue capacity must be positive");
        }
        this.queueCapacity = queueCapacity;
        this.policy = Objects.requireNonNull(policy);
        this.clock = Objects.requireNonNull(clock);
        this.agingNanos = Objects.requireNonNull(agingInterval).toNanos();
        if (agingNanos <= 0) throw new IllegalArgumentException("Aging interval must be positive");
        journal = store == null ? null : new Journal(store, probe);
        try {
            if (journal != null) {
                List<Journal.Record> records = journal.recover();
                long pending = records.stream().filter(r -> r.state() == State.QUEUED).count();
                if (pending > queueCapacity) throw new IllegalArgumentException("Recovered queue exceeds capacity; restart with capacity >= " + pending);
                for (Journal.Record record : records) {
                    Entry entry = new Entry(record, clock.getAsLong());
                    if (entry.state == State.RUNNING) {
                        entry.state = State.UNRESOLVED;
                        entry.error = "Process ended before completion was durably recorded; not automatically retried";
                        journal.append(entry.record());
                    }
                    registry.put(entry.id, entry);
                    if (entry.state == State.QUEUED) queue.add(entry);
                }
            }
        } catch (RuntimeException failure) {
            if (journal != null) journal.close();
            throw failure;
        }
        liveWorkers = workerCount;
        for (int i = 0; i < workerCount; i++) {
            Thread worker = new Thread(this::work, "thredder-worker-" + (i + 1));
            worker.setDaemon(true); // Uncooperative handlers cannot keep a CLI JVM alive after a reported timeout.
            workers.add(worker);
        }
        workers.forEach(Thread::start);
    }

    public Submission submit(String name, Job job) {
        return submit(name, 0, job);
    }

    public Submission submit(String name, int priority, Job job) {
        try { return admit(name, priority, job, 0, false, null); }
        catch (InterruptedException impossible) {
            Thread.currentThread().interrupt();
            throw new AssertionError(impossible);
        }
    }

    /** Wait for queue space within timeout; excess submitters reject without waiting for a permit. */
    public Submission offer(String name, int priority, Job job, Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "Timeout required");
        if (timeout.isNegative()) throw new IllegalArgumentException("Timeout must be nonnegative");
        final long nanos;
        try { nanos = timeout.toNanos(); }
        catch (ArithmeticException overflow) { throw new IllegalArgumentException("Timeout is too large", overflow); }
        return admit(name, priority, job, nanos, true, null);
    }

    public Submission offer(String name, int priority, JobDescriptor descriptor, Duration timeout) throws InterruptedException {
        Objects.requireNonNull(descriptor, "Job descriptor required");
        return admit(name, priority, descriptor.handler(), durationNanos(timeout), true, descriptor);
    }

    private static long durationNanos(Duration duration) {
        Objects.requireNonNull(duration, "Duration required");
        if (duration.isNegative()) throw new IllegalArgumentException("Duration must be nonnegative");
        try { return duration.toNanos(); }
        catch (ArithmeticException failure) { throw new IllegalArgumentException("Duration is too large", failure); }
    }

    public int activeSubmitters() { return maxSubmitters - submitters.availablePermits(); }

    private Submission admit(String name, int priority, Job job, long timeout, boolean interruptible, JobDescriptor descriptor)
            throws InterruptedException {
        if (priority < 0 || priority > 10) throw new IllegalArgumentException("Priority must be 0..10");
        if (name == null || name.isBlank() || job == null) {
            throw new IllegalArgumentException("Job name and handler are required");
        }
        if (journal != null && descriptor == null) throw new IllegalArgumentException("Persistent schedulers require a JobDescriptor, not a callback");
        if (interruptible && Thread.interrupted()) throw new InterruptedException();
        long start = System.nanoTime(); // Admission deadlines never use the injectable aging clock.
        if (!submitters.tryAcquire()) return new Submission(null, "Concurrent submitter limit reached");
        try {
            synchronized (gate) {
                while (true) {
                    if (interruptible && Thread.interrupted()) throw new InterruptedException();
                    if (!accepting) return new Submission(null, "Scheduler is closed");
                    long remaining = timeout - (System.nanoTime() - start);
                    if (timeout > 0 && remaining <= 0) return new Submission(null, "Admission timed out");
                    if (queue.size() < queueCapacity) break;
                    if (timeout == 0) return new Submission(null, "Queue is full");
                    TimeUnit.NANOSECONDS.timedWait(gate, remaining);
                }
                Entry entry = new Entry(name, job, priority, clock.getAsLong(), descriptor);
                if (journal != null) Journal.validateRecordSize(entry.record());
                persist(entry); // Durable acceptance precedes registry/queue publication and ACK.
                registry.put(entry.id, entry);
                queue.add(entry);
                gate.notifyAll();
                return new Submission(entry.id, null);
            }
        } finally { submitters.release(); }
    }

    public Optional<Snapshot> status(UUID id) {
        synchronized (gate) {
            Entry entry = registry.get(id);
            return entry == null ? Optional.empty() : Optional.of(entry.snapshot());
        }
    }

    public List<Snapshot> list() {
        synchronized (gate) {
            return registry.values().stream().map(Entry::snapshot).toList();
        }
    }

    public int queuedCount() { synchronized (gate) { return queue.size(); } }

    private long score(Entry entry, long now) {
        long age = Math.max(0, now - entry.enqueuedAt) / agingNanos;
        return age > Long.MAX_VALUE - entry.priority ? Long.MAX_VALUE : age + entry.priority;
    }

    // Rescan under the admission lock: no stale heap keys as waiting priorities change.
    private Entry next() {
        int best = 0;
        if (policy == Policy.PRIORITY_AGING) {
            long now = clock.getAsLong();
            for (int i = 1; i < queue.size(); i++) {
                if (score(queue.get(i), now) > score(queue.get(best), now)) best = i;
            }
        }
        return queue.remove(best); // Equal scores keep submission order.
    }

    private void persist(Entry entry) {
        if (journal == null) return;
        try { journal.append(entry.record()); }
        catch (RuntimeException failure) {
            storageError = failure.toString();
            accepting = false; stopDispatch = true; gate.notifyAll();
            throw failure;
        }
    }

    private void work() {
        try { workLoop(); }
        finally {
            synchronized (gate) {
                liveWorkers--;
                releaseStoreIfStopped();
                gate.notifyAll();
            }
        }
    }

    private void workLoop() {
        while (true) {
            Entry entry;
            synchronized (gate) {
                while (queue.isEmpty() && accepting && !stopDispatch) {
                    try { gate.wait(); }
                    catch (InterruptedException interrupted) { /* Recheck scheduler state. */ }
                }
                if (stopDispatch || queue.isEmpty()) return;
                entry = next(); gate.notifyAll();
                if (!entry.claim.tryClaim()) throw new IllegalStateException("Job already claimed");
                transition(entry, State.QUEUED, State.RUNNING);
                entry.startedAt = Instant.now();
                entry.queueWaitNanos = Math.max(0, clock.getAsLong() - entry.enqueuedAt);
                Thread.interrupted();
                try { persist(entry); } // Never execute a handler without its durable RUNNING boundary.
                catch (RuntimeException failure) {
                    entry.state = State.UNRESOLVED; entry.error = "Start record could not be committed";
                    entry.job = null; return;
                }
                entry.runner = Thread.currentThread();
            }
            Throwable failure = null;
            try { entry.job.execute(); }
            catch (Throwable error) { failure = error; }
            finally {
                synchronized (gate) {
                    // Avoid closing an interruptible file channel because a handler left its flag set.
                    Thread.interrupted();
                    entry.runner = null;
                    entry.error = failure == null ? null : failure.getClass().getSimpleName() + ": " + failure.getMessage();
                    if (entry.error != null && entry.error.length() > 8000) entry.error = entry.error.substring(0, 8000);
                    entry.finishedAt = Instant.now();
                    if (entry.state != State.RUNNING && entry.state != State.UNRESOLVED)
                        throw new IllegalStateException("Completion without ownership");
                    entry.state = failure == null ? State.SUCCEEDED
                        : (entry.cancellationRequested && failure instanceof InterruptedException ? State.CANCELLED : State.FAILED);
                    try { persist(entry); }
                    catch (RuntimeException storageFailure) {
                        entry.state = State.UNRESOLVED; entry.finishedAt = null;
                        entry.error = "Handler ended but completion record could not be committed";
                    }
                    entry.job = null; gate.notifyAll();
                }
            }
        }
    }

    private void releaseStoreIfStopped() {
        if (journal != null && !journalClosed && !accepting && liveWorkers == 0) {
            journalClosed = true;
            try { journal.close(); }
            catch (RuntimeException failure) { storageError = failure.toString(); }
        }
    }

    private static void transition(Entry entry, State expected, State next) {
        if (entry.state != expected) throw new IllegalStateException("Illegal job transition");
        entry.state = next;
    }

    /** Stop admission, drain for grace, interrupt unfinished handlers, then observe for cancellationWait. */
    public ShutdownReport shutdown(Duration grace, Duration cancellationWait) {
        long graceNanos = durationNanos(grace), cancelNanos = durationNanos(cancellationWait);
        if (workers.contains(Thread.currentThread())) throw new IllegalStateException("A worker cannot close its own scheduler");
        boolean interrupted = Thread.interrupted();
        synchronized (gate) { accepting = false; gate.notifyAll(); }
        try {
            if (!interrupted) waitForWorkers(graceNanos);
        } catch (InterruptedException callerInterrupted) { interrupted = true; }
        synchronized (gate) {
            if (liveWorkers > 0) {
                stopDispatch = true;
                for (Entry entry : queue) {
                    // A durable QUEUED job is preserved for restart; an ephemeral job cannot be recovered.
                    if (journal == null) {
                        entry.state = State.CANCELLED; entry.finishedAt = Instant.now();
                        entry.error = "Cancelled before execution at shutdown deadline";
                    }
                    entry.job = null;
                }
                queue.clear();
                for (Entry entry : registry.values()) {
                    if (entry.runner != null && !entry.cancellationRequested) {
                        entry.cancellationRequested = true;
                        entry.state = State.UNRESOLVED;
                        entry.error = "Cancellation requested; handler termination not yet observed";
                        // Interrupt only while holding the monitor, before the worker can commit completion.
                        try { persist(entry); } catch (RuntimeException storageFailure) { /* Report fault below. */ }
                        entry.runner.interrupt();
                    }
                }
                gate.notifyAll();
            }
        }
        try {
            if (!interrupted) waitForWorkers(cancelNanos);
        } catch (InterruptedException callerInterrupted) { interrupted = true; }
        ShutdownReport report;
        synchronized (gate) {
            releaseStoreIfStopped();
            report = new ShutdownReport(workers.stream().noneMatch(Thread::isAlive),
                registry.values().stream().filter(e -> e.state == State.QUEUED).map(e -> e.id).toList(),
                registry.values().stream().filter(e -> e.state == State.UNRESOLVED).map(e -> e.id).toList(), storageError);
        }
        if (interrupted) Thread.currentThread().interrupt();
        return report;
    }

    private void waitForWorkers(long budget) throws InterruptedException {
        long start = System.nanoTime();
        synchronized (gate) {
            while (liveWorkers > 0) {
                long remaining = budget - (System.nanoTime() - start);
                if (remaining <= 0) return;
                TimeUnit.NANOSECONDS.timedWait(gate, remaining);
            }
        }
        for (Thread worker : workers) {
            long remaining = budget - (System.nanoTime() - start);
            if (remaining <= 0) return;
            TimeUnit.NANOSECONDS.timedJoin(worker, remaining);
        }
    }

    /** Convenience shutdown: five seconds to drain, one second to observe cancellation. */
    @Override public void close() { shutdown(Duration.ofSeconds(5), Duration.ofSeconds(1)); }
}
