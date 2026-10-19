# Thredder portfolio and interview walkthrough

Thredder is a backend-only Java CLI job scheduler built to make concurrency decisions visible and testable. v1.0 completes the original learning plan. It is a local portfolio release, not a production-service or performance certification.

## Start from a clean checkout

Install a JDK 17 or later, then run:

```sh
./thredder build
./thredder test
./thredder run --workers 2 --capacity 8 --policy aging --aging-ms 1000 --offer-ms 5000
```

The launcher uses `JAVA_HOME`, Homebrew OpenJDK, then `PATH`. No dependency download is needed. It is verified on the current macOS/OpenJDK 27 environment; Java 17 API/source compatibility is enforced by compilation, but execution on Java 17 itself and Windows have not been verified.

Enter `submit sleep 1000`, `priority 10 sleep 100`, and `list`; copy an accepted UUID into `status <UUID>`. Use `submit fail demonstration` to inspect failure reporting. `exit` starts deadline-based shutdown. A job is a whole scheduling unit; concurrency is across jobs, not automatic parallel decomposition inside one job.

## Architecture to explain

The CLI validates commands and calls admission. A permit bounds concurrent submitters; a monitor protects a bounded pending queue, registry and lifecycle changes. Fixed worker threads claim jobs, run handlers outside that monitor and record outcomes. An optional journal forces acceptance/start/completion records. A separate receiver owns the business effect and its idempotency record.

The useful distinction is between four boundaries: admission, ownership, durable scheduler state, and durable business effects. Solving one does not automatically solve the others. A completed scheduler job is not proof of an external transaction's result, and a failed scheduler job can follow a successful receiver commit if the acknowledgment was lost.

## Experiments and evidence

Run `./thredder test` for all experiments; it exits nonzero on failure and uses bounded waits plus a suite watchdog. Test sources are deliberately separate from working scheduler paths.

| Problem | Reproduction and fix | Evidence to inspect | Tradeoff to explain |
| --- | --- | --- | --- |
| Race / duplicate claim | A barrier lets two callers pass an unsafe check before either writes. Replace it with compare-and-set ownership. | `ConcurrencyTest.claims`: two unsafe winners, one winner among 100 safe attempts; `AtomicClaim` in scheduler dispatch. | An ownership token is not rollback or an idempotency key. The queue monitor already prevents duplicate dequeue; the token makes ownership explicit. |
| Deadlock | Two threads hold opposite first locks and wait for each other's second lock. Observe the cycle, then interrupt the fixture. Acquire all declared resources in one global ID order. | `ConcurrencyTest.deadlockFixture`, `resourceLocks`; `./thredder demo` completes 100 opposing transfers. | All participants must use the same manager/order; nested acquisition is rejected. Lock ordering does not roll back partial business mutations. |
| Starvation | Feed high-priority arrivals while a low-priority job waits. Advance a fake clock; age the low-priority job until it wins. | `ConcurrencyTest.agingWithArrivals`: the old job runs at the tenth arrival while arrivals continue to fifteen. | Non-preemptive fairness assumes finite service, bounded priorities and worker/clock progress. Scanning the queue is simple but O(capacity). |
| Overload | Hold workers and fill the queue; start a timed submitter, then add excess callers. | `AdmissionTest`: cap rejection, timeout, space wakeup, interruption and close races under both policies. | Queue bounds do not bound waiting callers; both need limits. Blocking admission shifts delay to producers. It is not FIFO-fair or hard real-time. |
| Shutdown | Hold a running job past the grace period. Compare an interruptible handler with one deliberately ignoring interruption. | `RecoveryTest.cooperativeShutdown`, `stubbornShutdown`, `interruptedShutdown`. | An interrupt is a request. `CANCELLED` requires observed cooperative exit; a live handler is `UNRESOLVED`. Daemon workers do not keep an exiting application alive. |
| Recovery | Halt disposable child JVMs before/after acceptance, start and completion, plus a torn write. | `RecoveryTest.crashBoundaries`, corruption, capacity and storage-failure checks. | Resume only durable queued work. Unknown running outcomes require reconciliation. Corrupt data fails closed without discarding records. |
| Duplicate effect after retry | Commit at the receiver, deliberately lose the response, then retry the same key from a different job/attempt. | `./thredder retry-demo` and `IdempotencyTest`: concurrent duplicates, payload conflict, rollback and receiver crash tests. | Only the receiver can atomically cover its own effect and deduplication record. No arbitrary exactly-once claim. |

## Walk through durable shutdown

```sh
mkdir -p data
./thredder run --store data/jobs.thr --workers 1 --shutdown-ms 100 --cancel-ms 1000
```

Submit `sleep 60000`, then `sleep 0`, and exit. With the first job running, the deadline interrupts it and preserves pending durable work. Reopen the same store and inspect the same IDs. The queued job resumes; a cooperatively stopped job stays cancelled. Crash tests cover the more difficult case in which the old process never recorded a running job's outcome: it remains unresolved and is not blindly replayed.

The store requires one owner and supported file/directory flush operations. Its journal and the in-memory history grow without compaction. Synchronous disk work under the monitor simplifies the acceptance boundary but affects latency and limits the strength of shutdown timing guarantees.

## Walk through safe retries

```sh
mkdir -p data
./thredder retry-demo data/receiver.thr interview-operation-001
./thredder retry-demo data/receiver.thr interview-operation-001
```

On a new receiver, both runs leave A=9900 and B=10100. Each run prints different job/attempt IDs but the same logical key. The first attempt intentionally reports failure after receiver commit; the retry succeeds with the saved result. Reusing that key with different transfer details is rejected. A new key would mean a new operation, not a retry.

The receiver writes balances and completion in one atomic snapshot. A crash before replacement leaves both unchanged; a crash after commit may lose the response but preserve both. Keys are retained indefinitely in this demo. External APIs need an equivalent contract with a documented retention window; a scheduler record alone cannot supply it.

## Present measurements honestly

See the [benchmark report](../benchmarks/README.md) for the full method, numbers and raw data. Reproduce with:

```sh
./thredder benchmark benchmarks/interview-run
python3 tools/summarize_benchmarks.py benchmarks/interview-run
```

The retained run has 128 trials, 96 measured, and 32,768 per-submission records. In the selected blocking workload, moving from one to four workers improved throughput, while a larger queue mainly increased wait time. Immediate rejection and timed admission cannot be compared using accepted-job latency alone because they admitted different proportions of the burst. Aging's fairness comes from the deterministic test's assumptions, not from a throughput chart.

Do not claim production capacity, a universal worker count, starvation freedom under arbitrary load, crash-proof external effects or exactly-once execution. Discuss what a next experiment would isolate: multiple producers, persistent-store latency, longer trials, independent JVM forks or receiver snapshot cost.

## What v1.0 deliberately leaves open

No frontend, HTTP API, distributed scheduling, cron/calendar, automatic job splitting, benchmark-derived default configuration, journal compaction/migrations or production monitoring is included. Durable descriptors currently allow only sleep/fail jobs; custom Java callbacks are in-memory. Retention and receiver snapshot rewriting need further engineering for sustained service use. The Notion plan is historical; the repository roadmap records implementation status.
