# Thredder

Thredder is a backend-only concurrent job scheduler with a long-running CLI. It is a learning and portfolio project for concurrency and Atlassian interview preparation.

**v1.0.0 completes the learning and portfolio plan:** a fixed worker thread pool, bounded thread-safe FIFO queue, unique job IDs, lifecycle tracking, `submit`/`list`/`status` commands, and tests. Java is now the implementation language for this version. Atomic claims, ordered resource locking, and bounded priority scheduling with aging are implemented. Deadline-based shutdown, cooperative cancellation and optional durable recovery are also implemented. The local idempotency demo, reproducible benchmarks and interview walkthrough are also complete. See the [roadmap](ROADMAP.md) for the completed scope and optional future hardening.

## Embeddable library preview — 1.1.0-SNAPSHOT

The `plug-in` branch adds an application-owned Java library with registered job handlers. Your application supplies the work; Thredder supplies bounded scheduling, lifecycle tracking and optional recovery. The CLI remains available. This is an embeddable extension API, not an IDE or framework-specific plugin.

```sh
./thredder test
./thredder package
```

Packaging produces separate core, CLI and file-hasher example JARs in `build/libs`. See the [embedding guide](docs/EMBEDDING.md) for application code, handler contracts, recovery and a runnable file-processing example. These are local preview artifacts; no package-registry publication is claimed.

## Final release guide

- [Portfolio walkthrough](docs/WALKTHROUGH.md): reproduce each failure, explain its fix and discuss the limits.
- [Benchmark report](benchmarks/README.md): measured results, method, raw trials and per-job samples.
- [Changelog](CHANGELOG.md) and [roadmap](ROADMAP.md): delivered scope and remaining hardening opportunities.

v1.0 is the completed learning/portfolio version. Production readiness and exactly-once execution are not claimed.

Licensed under the [MIT License](LICENSE).

## Build and run

Requires a JDK version 17 or later and Bash on macOS/Linux. No third-party libraries, package downloads, Maven or Gradle are required. The launcher uses `JAVA_HOME` first, then Homebrew's `/opt/homebrew/opt/openjdk`, then Java on `PATH`.

```sh
./thredder build
./thredder test
./thredder run --workers 2 --capacity 16
```

For a JDK installed elsewhere, set `JAVA_HOME` to its installation directory. Sources compile with `--release 17`; verification in this workspace used Homebrew OpenJDK 27. Running on JDK 17 itself has not been verified. The shell launcher has not been tested on Windows.

Enter commands in the same running process:

```text
submit sleep 1000
submit sleep 500
list
status <UUID returned by submit>
submit fail demonstration failure
list
help
exit
```

Replace the UUID placeholder with the actual accepted ID. A later `status` or `list` shows the completed result. The CLI accepts one command per line; it prints no prompt so piped input stays readable. Blank lines are ignored. Job types available are `sleep` (0–60000 milliseconds) and `fail` (a message). In-memory Java callers can submit other handlers through `Scheduler.Job`. Durable schedulers accept only validated `JobDescriptor` values.

`submit` returns `ACCEPTED <UUID>` or `REJECTED <reason>`. Full queues reject immediately by default; `--offer-ms` enables timed waiting. `list` shows every accepted job in submission order; `status` shows one job, including its failure detail. Invalid commands print `ERROR`; unknown IDs print `NOT FOUND`. Invalid startup options exit with code 2. `exit` and end-of-input stop admission and begin deadline-based shutdown, then print a shutdown report.

## Architecture and guarantees

```text
CLI → Scheduler.offer → bounded pending queue → N fixed worker threads
              ↓                                    ↓
          registry ← synchronized lifecycle updates → optional forced journal
              ↓
       immutable list/status snapshots
```

- The pending queue is a bounded list protected by the scheduler monitor. Capacity counts waiting jobs, separately from up to N executing jobs. Idle workers wait on the monitor and are notified on submission or close; they do not busy-spin.
- A monitor coordinates admission, registry visibility and state transitions. Handlers run outside that monitor. A worker dequeues each accepted entry once during normal process execution.
- Normal lifecycle: `QUEUED → RUNNING → SUCCEEDED` or `FAILED`. Shutdown and recovery add `CANCELLED` and `UNRESOLVED`. Internal code owns state transitions; snapshots contain submission/start/finish timestamps and error details.
- The FIFO guarantee concerns dequeue order, not completion order. With multiple workers, observed start order may also differ because the operating system schedules threads independently.
- Handler failures are recorded and isolated so the worker can process later jobs. The boundary also captures handler errors such as assertions, but recovery from fatal JVM/resource failures is not guaranteed.
- Each job is one indivisible scheduling unit. Jobs run concurrently with one another; there is no automatic parallel splitting, preemption, frontend, HTTP service or calendar/cron scheduling.

## Current limits

Without `--store`, state is in memory and is lost on process exit. With a store, validated job descriptions and lifecycle history survive restart subject to the storage guarantees below. Job IDs identify submissions; they are not idempotency keys. The scheduler does not automatically retry uncertain work or provide transactional business effects. The separate v0.5 receiver demo owns a local atomic transfer transaction; neither component claims arbitrary exactly-once execution.

Default shutdown drains for five seconds and observes cooperative cancellation for one more second. An uncooperative handler may remain alive and is reported as unresolved. Workers are daemon threads: once the CLI exits, remaining handlers do not keep its JVM alive. This is not safe forced thread termination, and it does not imply rollback. Java clients must explicitly await/report shutdown rather than relying on worker threads to keep their application running. A handler cannot close its own scheduler. Ctrl-C/forced termination is treated as a crash, not as the graceful shutdown path.

The pending queue is bounded, but retained job history is not: long-running sessions with many completed jobs can grow memory use. Completed handler references are released. Concurrent admission calls are capped (default 16), including callers waiting for queue space. Do not treat this learning implementation as a production service.

## Verification

`./thredder test` compiles with warnings treated as errors and runs a dependency-free test suite. Assertions are explicit and do not depend on Java's `-ea` flag. A 45-second watchdog prevents a regression from hanging the entire test process.

Coverage includes controlled queue saturation; FIFO with one worker; three workers executing simultaneously without exceeding their limit; 200 concurrent submissions with unique IDs and per-job execution counts; lifecycle snapshots and completion metadata; failure/interrupt isolation; admission racing shutdown; draining and repeated close; CLI commands, bad input, unknown IDs, EOF and rejection; invalid configuration; and no surviving scheduler workers. Coordination uses latches and bounded waits rather than sleep-based ordering assumptions.

Tests also launch child JVMs that halt at acceptance, start and completion boundaries, including a torn-write scenario. These verify the specified recovery paths; they do not prove correctness for every storage failure, external effect, JVM failure or thread interleaving.

See [CHANGELOG.md](CHANGELOG.md) for delivered changes and [ROADMAP.md](ROADMAP.md) for staged work and acceptance criteria.

## v0.2 policies and experiments

FIFO remains the default. Enable priority aging explicitly:

```sh
./thredder run --workers 2 --capacity 16 --policy aging --aging-ms 1000
```

```text
priority 0 sleep 100
priority 10 sleep 100
priority 5 fail example failure
list
exit
```

Ordinary `submit` uses priority zero. Priorities range from 0 to 10 (higher wins), and `list`/`status` display the base priority. FIFO ignores priority. Scheduling is non-preemptive: a newly submitted high-priority job cannot interrupt running work.

At each dequeue, aging selects the maximum of `basePriority + floor(waitNanos / agingIntervalNanos)`, with FIFO ties. The scheduler recomputes scores by scanning pending jobs under the monitor; it does not mutate heap keys. Selection costs O(queue capacity), a deliberate simplicity tradeoff for this learning version. An injected monotonic clock makes boundary tests deterministic. Score addition saturates to avoid overflow; elapsed intervals must remain below 292 years.

A job waiting ten aging intervals reaches at least the maximum fresh priority. Under a progressing clock, finite job runtimes, eventual worker scheduling, bounded base priorities and a bounded queue, it eventually outranks later arrivals. Earlier contenders can still run first; this is not a wall-clock deadline, preemption, or a guarantee when all workers are stuck. Aging does not guarantee admission to a full queue.

Run the experiments:

```sh
./thredder test
./thredder demo
```

- **Atomic claims:** the tests use a barrier to force two unsafe check-then-write claims to succeed. The fixed `AtomicClaim` uses compare-and-set; 100 competing attempts produce one owner. Scheduler dispatch uses the safe claim. Its monitor already serializes dequeue, so the claim is an explicit ownership invariant rather than a claim that the old FIFO queue was unsafe.
- **Deadlock:** the isolated test observes two threads queued on each other's held locks, then interrupts and joins them. No unsafe algorithm is used in the scheduler. `ResourceLockManager` deduplicates resource IDs, sorts them lexicographically, acquires interruptibly and releases in reverse order. Tests cover exceptions, interrupted partial acquisition, interrupted handlers and nested-acquisition rejection.
- **Ordered transfer demo:** 100 opposing operations on two shared resources complete with both balances unchanged. All participants must use the same manager and declare their complete resource set in one call. Nested calls are rejected to preserve ordering. Resource-lock entries are retained; use a finite resource domain or add lifecycle management before production use.
- **Aging:** fake-clock tests verify priority ordering, FIFO ties, bounded capacity under both policies, and a priority-zero job running after ten higher-priority arrivals while the stream continues to fifteen.

Atomic ownership and lock ordering are not rollback or exactly-once semantics. An exception after one business mutation can leave a partial effect; the transfer example is an in-memory synchronization exercise, not a transactional financial service. Deadline-based cancellation and durable recovery are described below; the v0.5 local receiver demonstrates safe retries; external systems still require their own compatible contract.

## v0.3 timed admission and backpressure

```sh
./thredder run --workers 2 --capacity 16 --submitters 8 --offer-ms 5000
```

`--offer-ms` applies to both `submit` and `priority` commands. The default is zero (immediate admission or rejection); 5000 milliseconds is an example. `--submitters` must be positive and defaults to 16. The CLI reads commands serially, so a waiting submission delays reading the next command, including `exit`. Concurrent callers use the Java API; the CLI does not spawn unbounded submission threads.

```java
Scheduler.Submission result = scheduler.offer(
    "example", 5, () -> doWork(), Duration.ofSeconds(5));
```

`offer` throws `InterruptedException` if interrupted before acceptance. A negative or unrepresentably large timeout is rejected. `submit` remains the immediate API. Both methods acquire a submitter permit without waiting; excess callers receive `Concurrent submitter limit reached`. Calls holding permits include those contending for the internal monitor and those waiting for queue space. No executor or extra waiter queue is created for rejected callers. The caller remains responsible for bounding its own thread creation.

Queue-space waits use elapsed `System.nanoTime()` and a remaining-time loop, independently of the injectable aging clock. Spurious notifications do not reset the budget. Zero timeout gives an immediate attempt; positive timeouts reject with `Admission timed out` when the budget has expired. Operating-system scheduling, internal monitor sections and synchronous journal I/O can delay the actual return beyond the requested timeout, so this is not a hard real-time deadline. Admission is not FIFO-fair among competing callers.

Removing a queued job wakes waiting submitters. Closing the scheduler stops admission and wakes them with `Scheduler is closed`, even while accepted jobs are still draining. Acceptance, registry insertion and queue insertion share one monitor boundary with shutdown. Normal rejections and pre-acceptance interruptions create no job record. In persistent mode, a storage failure or interruption during journal I/O may leave an uncertain durable record; the scheduler stops admission and reports a storage error instead of acknowledging acceptance. All permit-holding exits release the permit, including exceptions. Interruption racing after acceptance does not cancel an already accepted job.

Tests cover timeout on a full queue, zero timeout, capacity becoming available, immediate cap rejection, interruption, shutdown wakeup before drain completion, permit reuse after errors, and concurrent admission/close accounting under both policies. Existing v0.1/v0.2 tests remain enabled. Timed admission and shutdown use separate budgets. A worker that submits back into a full scheduler can occupy its worker slot until its offer times out; avoid nested blocking submissions.

## v0.4 shutdown and persistence

```sh
mkdir -p data
./thredder run --store data/jobs.thr --shutdown-ms 5000 --cancel-ms 1000
```

The store's parent directory must already exist. The CLI uses the same `submit`, `priority`, `list` and `status` commands in durable mode. Reopen the same path to recover. Configuration is supplied afresh on restart; only job data is stored. Pending jobs retain their IDs and submission order, but monotonic aging starts again on restart. A queue capacity smaller than the recovered pending count is rejected before jobs start; restart with sufficient capacity.

### Shutdown contract

`shutdown(grace, cancellationWait)` stops admission immediately, waits up to the grace budget for workers to drain, then stops further dispatch, requests interruption of remaining running handlers, and observes them for the cancellation-wait budget. Both durations may be zero. `close()` uses five seconds plus one second. Java users who need the result should call `shutdown` and inspect its immutable report.

| Work at the deadline | Outcome |
| --- | --- |
| Queued, in-memory only | `CANCELLED`, never executed |
| Queued, durable | Kept `QUEUED` in the journal for next startup; reported in `pending` |
| Running, interruption acknowledged by exiting with `InterruptedException` | `CANCELLED` only after handler cleanup/exit is observed |
| Running, returns normally after an interrupt request | `SUCCEEDED`; a cancellation request is not proof of cancellation |
| Running, ignores interruption | `UNRESOLVED`; reported in `unresolved`, with `terminated=false` |
| Completion cannot be durably recorded | `UNRESOLVED` and a reported storage error |

Handler `finally` blocks and the resource lock manager release resources during cooperative exit. No unsafe thread-stop API is used. A later shutdown/status check can observe an unresolved live handler eventually finishing. Repeated and concurrent shutdown calls are safe; caller interruption triggers cancellation and is restored before return. A journal stays locked until all of its scheduler's workers actually finish, so another scheduler cannot recover work while an old handler is still running.

The report includes worker termination, durable pending IDs, unresolved IDs and a storage error if present. CLI exit status is 0 for a clean shutdown (possibly with pending durable work), 2 for invalid arguments, 1 for startup/store-open failure, and 3 for unresolved work, live workers or storage faults reported at shutdown. Shutdown budgets bound waiting for handlers, not blocking disk operations or OS scheduling; this is not a hard real-time guarantee.

### Durable acceptance and recovery

The journal uses versioned, length-delimited records with CRC32 checksums and full job snapshots. Each lifecycle append is flushed with `FileChannel.force(true)` before publication. Store creation also forces its parent directory. A single file lock enforces one live owner per journal. Storage must support these file/directory flush and lock operations; unsupported filesystems fail instead of silently weakening the contract. This was tested on the current local macOS filesystem, not network storage or simulated power loss.

- **Acceptance:** flush `QUEUED` before returning an accepted ID or exposing the job to workers. A crash after the flush but before acknowledgment can leave an accepted job whose caller did not receive the ID; scheduler submissions are not deduplicated; use a stable receiver operation key for repeatable effects.
- **Execution:** flush `RUNNING` before invoking the handler. Durable `QUEUED` jobs therefore have not crossed the execution boundary and may resume automatically.
- **Completion:** flush the terminal outcome before exposing it as durably completed. Completion records do not atomically cover arbitrary business side effects.
- **Restart:** restore completed/cancelled/failed records unchanged; resume `QUEUED`; change recovered `RUNNING` to `UNRESOLVED`. Existing unresolved records remain unresolved. Inspect/reconcile these manually; the scheduler has no automatic retry command for these records; the separate retry demo explicitly resubmits a receiver operation with the same key.
- **I/O failure:** stop admission/dispatch, expose the fault and leave uncertain outcomes unresolved. An acceptance call that throws may still have a durable record; do not infer that it is safe to resubmit.
- **Corrupt or incomplete data:** reject the entire startup before executing jobs, preserve the file unchanged, and report the error. There is no automatic truncation or repair that could discard accepted work. Preserve the file and restore a known-good copy or reconcile records before restarting.

The CLI persists the built-in `sleep` and `fail` descriptors. Embedded applications can persist custom descriptor types explicitly registered through `JobRegistry`; every recovered type, including completed history, must still be registered. Arbitrary callbacks, Java object graphs and thread objects are never serialized. New durable job types require explicit descriptor validation/handler code. The journal format is versioned but has no migration facility yet. Individual text fields use the journal codec's 65,535-byte encoded limit; oversized acceptance data is rejected before writing. Failure details are truncated to 8,000 characters to keep completion records bounded.

Journal files and the in-memory history grow without compaction. Persistence serializes disk I/O under the scheduler monitor, trading throughput for a simple auditable acceptance boundary; benchmarks and retention/compaction are future work. CRC32 detects accidental corruption and is not an authenticity mechanism. Keep journals private and treat this as a local learning implementation.

`RecoveryTest` verifies cooperative/uncooperative and interrupted shutdown, resource release, preserved queued work, repeated restart, exclusive ownership, descriptor validation, recovery-capacity checks, injected storage faults, six before/after durable-boundary crashes plus a torn-write crash, and checksum/version/truncation failures. Existing concurrency and admission tests also remain enabled.

## v0.5 idempotency and safe-retry demo

```sh
mkdir -p data
./thredder retry-demo data/receiver.thr transfer-example-001
```

Run the same command again with the same store and key: balances remain A=9900, B=10100. A different key denotes a new transfer of 100 units from A to B. This receiver starts with 10,000 integer demo units in each account; it is not a banking integration.

The demo submits two scheduler jobs with distinct job IDs and distinct attempt IDs for **one stable operation key**. Attempt one commits at the receiver and then deliberately loses its response, leaving a FAILED scheduler job despite a successful receiver effect. Attempt two resends the exact request with the same operation key and obtains the originally recorded result. No scheduler job state is silently rewritten, and no unresolved v0.4 job is automatically retried.

`IdempotentReceiver.transfer(Request)` is also available to Java callers. Requests bind a case-sensitive key to the exact `(from, to, amount)` payload. Concurrent requests on the shared receiver serialize validation, uniqueness checking and commit. A duplicate returns the original result, including original post-transfer balances, even if later operations changed current balances. A conflicting payload with the same key is rejected. Never generate a new key merely because a response is missing.

### Transaction boundary

The receiver owns both its business state and deduplication records. It stages the new balances, key, payload and result in memory, writes a checksummed full snapshot to a temporary file in the same directory, flushes it, atomically replaces the receiver file, and forces the parent directory before acknowledging success. There is no fallback to a non-atomic file move. A separate stable lock file prevents multiple live receiver owners while snapshots are replaced.

Validation or a failure before replacement leaves both the prior balances and completion history unchanged. A failure after replacement can mean the transaction committed even when the caller saw an error. The receiver then becomes unavailable until reopened; retry the same key and payload to reconcile. A process crash recovers either the previous snapshot or the committed snapshot, with effects and completion together. Corrupt snapshots fail closed without rewriting the file. This depends on the local filesystem honoring atomic replacement, flush and lock operations; tests exercise process crashes on this machine, not arbitrary power loss or network filesystems.

`lookup(key)` is a local observation, not a distributed proof that a request never ran. A missing record can precede another caller's in-flight request. Correctness comes from atomic deduplication inside `transfer`, not a client-side check-then-submit sequence.

### Retention and external receivers

Keys and recorded results are retained indefinitely in v0.5. There is no expiry, pruning, migration or compaction; the full snapshot is rewritten per new operation and grows with history. This is an auditable learning design, not a throughput optimization. Crashes before replacement may leave `.pending` files, which are ignored on reopen; preserve them when investigating storage incidents. Keep the receiver and scheduler journals at distinct paths and do not delete an active receiver's `.lock` file.

For a real external service, safe retries require that service to bind the same stable key to the same payload and atomically commit its effect and completion result. Document its retention window and retry semantics; once it forgets a key, a later retry can repeat the effect. Without such a contract, reconcile uncertain operations rather than blindly resubmitting. A scheduler completion record cannot atomically cover an unrelated remote side effect, and this project does not claim exactly-once execution.

`IdempotencyTest` checks 100 concurrent duplicates, payload conflicts, saved-result retention across later transfers/restarts, insufficient-funds rollback, exclusive ownership, corrupt-file rejection, injected pre/post-commit failures, lost responses, and three child-JVM crashes around replacement/acknowledgment.

## v1.0 measurement

`./thredder benchmark <new-directory>` runs the 32-configuration CPU/blocking matrix and preserves all warmup/measured trials plus per-submission timings. `python3 tools/summarize_benchmarks.py <directory>` validates the raw accounting and generates median summaries; Python is optional for this reporting step, not required by the scheduler.

Runtime snapshots now expose nullable `queueWaitNanos` telemetry. It measures monotonic enqueue-to-dispatch time and is not persisted; records restored from a previous process have no historical queue timing. The benchmark measures end-to-end handler latency separately. See the benchmark report for precise definitions and limits.
