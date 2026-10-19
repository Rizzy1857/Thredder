# Changelog

User-visible changes are recorded by version. Unreleased plans belong in the roadmap; entries below describe implemented work.

## 1.0.0 — 2026-10-19

### Added

- Reproducible CPU/blocking benchmark matrix across worker counts, queue capacities, FIFO/aging and immediate/timed admission.
- Raw environment, trial and per-submission evidence: 128 trials, 96 measured, 32 configurations.
- Data validation and median-summary tool, benchmark report and explicit measurement limitations.
- Nullable monotonic queue-wait telemetry on runtime snapshots, with a deterministic boundary test; telemetry is not persisted.
- Complete portfolio/interview walkthrough covering races, deadlocks, starvation, overload, shutdown, recovery and safe retries.

### Milestone

- All original tasks T01–T14 are implemented and verified locally. This is the final learning/portfolio milestone, not a production-service certification.
- Retention, compaction/migration, multi-producer and persistence benchmarks, monitoring and deployment hardening remain optional follow-ups.

## 0.5.0 — 2026-10-19

### Added

- Local idempotent transfer receiver with stable operation keys, exact payload binding and recorded results.
- Atomic full-snapshot transactions containing balances, operation key and completion result, with flush, checksum and exclusive ownership.
- Duplicate requests return the original result; conflicting payloads reject; pre-commit failures leave both effects and completion unchanged.
- Runnable `retry-demo` with distinct job/attempt IDs, a simulated lost acknowledgment and a same-key retry.
- Concurrent duplicate, rollback, corruption, restart and three receiver crash-boundary tests.
- Retention, reconciliation and external receiver contract documentation.

### Limits

- Demonstration receiver only: scheduler jobs are not deduplicated and unresolved work is not automatically retried.
- Receiver keys are retained indefinitely; full-snapshot rewriting is not optimized for throughput.
- No arbitrary exactly-once execution claim or atomicity across unrelated external systems.

## 0.4.0 — 2026-10-19

### Added

- Configurable drain and cancellation-observation budgets, cooperative interruption, and immutable shutdown reports.
- `CANCELLED` and `UNRESOLVED` states; uncooperative handlers are never falsely reported cancelled.
- Optional single-owner, versioned, checksummed append journal, with forced acceptance/start/completion records.
- Validated sleep/fail descriptors for durable jobs; restart restores queued and completed history and quarantines uncertain running work as unresolved.
- CLI `--store`, `--shutdown-ms`, `--cancel-ms`, shutdown reporting and nonzero status for unresolved work/storage failure.
- Child-JVM crash tests at acceptance/start/completion boundaries and torn-write tests; corrupt-data, ownership, storage-fault and recovery-capacity checks.

### Changed

- `close()` now drains for five seconds then observes cancellation for one second; use `shutdown` to choose budgets and inspect the report.
- Workers are daemon threads so a reported non-cooperative job cannot keep a CLI process alive forever. Java callers must manage scheduler lifetime explicitly.
- Pending in-memory jobs cancel at the deadline; pending durable jobs remain queued for restart.

### Limits

- Durable mode supports allowlisted descriptors only; no callback serialization, external-effect rollback, idempotency or uncertain-work retries.
- Torn/corrupt stores fail closed without automatic repair. Journal compaction and migration remain future work.
- Deadlines are not hard real-time bounds on disk I/O; persistence depends on filesystem flush/lock guarantees.

## 0.3.0 — 2026-10-19

### Added

- Interruptible `offer` API with a configurable queue-space timeout and explicit timeout rejection.
- Configurable concurrent-submitter cap (default 16), shared by immediate and timed calls. Excess callers reject without waiting for a permit.
- CLI `--offer-ms` and `--submitters` options; zero offer timeout preserves immediate behavior.
- Queue-space and shutdown wakeups, monotonic deadline accounting independent of the aging clock, and permit cleanup on every exit.
- Tests under FIFO and aging for timeout, available space, cap rejection, interruption, shutdown, exception cleanup and admission/close races.

### Limits

- CLI commands are serial; a timed submission delays the next command. Concurrent producers use the Java API.
- Admission timeouts are not hard real-time deadlines. No admission fairness, worker cancellation or shutdown timeout is introduced.
- Persistence and recovery remain scheduled for v0.4.

## 0.2.0 — 2026-10-19

### Added

- Atomic ownership claims in dispatch and a deterministic unsafe-versus-safe claim experiment.
- Shared ordered resource locking with ID deduplication, interruptible acquisition, reverse release and rejection of nested locking.
- Opposing-transfer demo, bounded deadlock reproduction, and exception/interruption cleanup tests.
- Opt-in bounded priority scheduling with waiting-time aging, FIFO ties and an injectable monotonic clock.
- CLI `--policy fifo|aging`, `--aging-ms N`, `priority <0..10> <sleep|fail> <argument>`, and base-priority display.
- Deterministic priority, tie, capacity and continued-arrival aging tests; `./thredder demo` launcher.

### Changed

- Replaced the fixed FIFO queue primitive with a monitor-protected bounded pending list. FIFO remains the default; aging rescans scores at dequeue rather than relying on stale heap ordering.
- Idle workers now wait for submission/close notifications.

### Limits

- Lock ordering prevents resource-order cycles only when all users share the manager and declare all resources together. It provides no rollback.
- Scheduling remains non-preemptive. Aging fairness assumes finite service times and continued worker/clock progress; no execution deadline is promised.
- Timed admission, scheduler cancellation, persistence and safe external retries remain planned.

## 0.1.0 — 2026-10-19

### Added

- Java scheduler with configurable fixed workers and a bounded FIFO queue.
- Immediate admission rejection when full or closed; unique UUIDs for accepted jobs.
- Thread-safe registry, immutable status snapshots, lifecycle timestamps and failure details.
- Long-running CLI with `submit sleep`, `submit fail`, `list`, `status`, `help` and `exit`.
- Handler failure isolation and normal-exit draining with worker joins.
- Dependency-free build/run/test launcher targeting Java 17 language/API compatibility.
- Automated concurrency, lifecycle, shutdown and CLI tests.
- README with usage, architecture and limitations; staged roadmap with acceptance criteria and dependencies.

### Decisions and limitations

- Adopted Java for v0.1; Java was only proposed during conceptual planning.
- Kept the initial scheduler in memory with immediate rejection on saturation.
- No frontend, automatic job splitting, persistence, retries, shutdown deadline, cancellation or exactly-once guarantee.
- This version identifies the local implementation milestone; no remote release or published package is claimed.
