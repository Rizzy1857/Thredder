# Thredder roadmap

Current version: **1.1.0-SNAPSHOT on `plug-in`**. The original v1.0 learning tasks T01–T14 and the embeddable library preview tasks T15–T18 are implemented. The preview includes registered custom durable jobs, a builder, metrics and a packaged file-hashing consumer. Package-registry publication and host-specific adapters remain future work; optional production hardening is outside these milestones. The historical Notion plan has not been updated by this implementation task; this repository records the current code state.

Statuses: **Done** means implemented and verified against the listed criteria. **Not started** means planned, not an active blocker. Dates and estimates for future releases are intentionally unset. Version labels below are proposed sequencing, not delivery commitments.

## v0.1 — Foundation and MVP · Done

| ID | Deliverable | Dependencies | Acceptance evidence |
| --- | --- | --- | --- |
| T01 | Choose language and bootstrap | None | Java adopted; JDK-only build and test launcher; `./thredder build` and `./thredder test` pass. |
| T02 | Jobs, IDs and lifecycle | T01 | Unique IDs under concurrent submission; immutable snapshots; QUEUED/RUNNING/SUCCEEDED/FAILED; transition checks and recorded errors. |
| T03 | Bounded queue and fixed workers | T02 | Saturation rejects; pending capacity holds; fixed active limit; FIFO with one worker; failures do not remove workers. |
| T04 | submit/list/status CLI | T03 | Interactive commands share one scheduler; accepted IDs, unknown IDs, failures and invalid input covered by tests. |
| T05 | Verify MVP | T04 | Seven test groups cover concurrency and CLI; per-job execution counts; no remaining worker threads; build and piped CLI smoke checks. |

### Foundation decisions

- Java with a Java 17 source/API target; build verified with installed OpenJDK 27.
- No external dependencies initially. The assertion-based test runner avoids dependency setup; evaluate JUnit and Maven/Gradle when test/build complexity warrants them.
- Default two workers and capacity sixteen are configurable demo defaults, not benchmark-derived recommendations.
- Use trusted queue primitives for the working MVP. Deliberately unsafe examples belong in isolated learning fixtures, not the production path.

## v0.2 — Concurrency experiments · Done

### T06: Reproduce and fix atomic claim races · Done

**Depends on:** T05. Build an unsafe check-then-act fixture and a safe atomic claim implementation.

- A controlled interleaving reproduces duplicate claims in the unsafe fixture.
- Concurrent claims yield one winner with the fix; only the safe path reaches the scheduler.
- Document why an atomic claim does not provide rollback of a business operation or exactly-once external execution.

### T07: Ordered resource locking · Done

**Depends on:** T06. Add multi-resource demonstration jobs with stable, deduplicated resource IDs.

- Reproduce opposite-order locking in an isolated, bounded harness that cannot hang the suite.
- Apply one global acquisition order and release locks on every exit path.
- Opposing operations complete under stress; exceptions and cancellation release held resources.
- Distinguish deadlock prevention from transaction rollback and business atomicity.

### T08: Priority scheduler with aging · Done

**Depends on:** T07. Introduce a bounded scheduling policy with deterministic tie-breaking and an injectable monotonic clock.

- Higher effective priority is selected first; equal priorities use FIFO ordering.
- Low-priority jobs eventually run in a documented finite-service workload with continued arrivals.
- Tests advance a fake clock; aging recomputes or reheapifies priorities safely.
- Preserve capacity bounds and record the assumptions behind fairness claims.

Verification: `./thredder test` runs deterministic duplicate-claim and deadlock fixtures, lock cleanup tests, priority/FIFO/capacity tests and an aging test with continuing arrivals. `./thredder demo` verifies opposing transfers. The CLI exposes policy, aging interval and per-job priority. See the README for assumptions and reproduction commands.

## v0.3 — Backpressure · Done

### T09: Timed offers and concurrent-submitter cap · Done

**Depends on:** T08.

- Full queues wait for capacity only within a configurable timeout; five seconds is an example, not a fixed requirement.
- Cap concurrent submitters, including waiting submitters; excess callers reject explicitly without unbounded waiting threads.
- Timeout, interruption and shutdown release permits and wake waiting submitters.
- Test admission races and prove no accepted jobs disappear.

Verification: `AdmissionTest` checks timed space availability, timeout, cap rejection, interruption, shutdown wakeup and exact accepted/rejected execution accounting for both scheduling policies. Existing immediate APIs remain available. CLI options expose the offer timeout and cap; the Java API supports concurrent producers.

## v0.4 — Shutdown and recovery · Done

### T10: Deadline-based shutdown and cooperative cancellation · Done

**Depends on:** T09.

- Stop admission, drain until a configurable deadline, then request cancellation via interruption/token.
- Record queued/running outcomes consistently and release resources.
- Report uncooperative jobs as unresolved; never mark them cancelled solely because an interrupt was sent.
- Test repeated shutdown and avoid unsafe forced thread termination.

### T11: Persistence and restart recovery · Done

**Depends on:** T10.

- Select a durable store and define precisely when submission is durably accepted.
- Persist serializable descriptors and lifecycle records, not thread objects or arbitrary closures.
- Restart restores queued jobs and preserves completed records; previously RUNNING jobs get explicit uncertain/recoverable treatment.
- Crash tests exercise acceptance/completion boundaries and incomplete or corrupt data.
- Do not blindly retry external side effects before T12's receiver contract exists.

Verification: `RecoveryTest` covers shutdown deadlines, cooperative cleanup, unresolved non-cooperative work, preserved queued descriptors, completed history, store ownership, capacity checks, storage faults, seven child-JVM crash scenarios, and corrupt/torn journal rejection. The CLI exposes `--store`, `--shutdown-ms` and `--cancel-ms`; shutdown reports pending/unresolved IDs and storage errors. The store is a single-owner forced append journal. Only queued work resumes; uncertain running work requires reconciliation. See README for the acceptance boundary and filesystem limitations.

## v0.5 — Safe retries · Done

### T12: Idempotency and atomic completion records · Done

**Depends on:** T11.

- Separate logical-operation idempotency keys from job IDs and attempt IDs.
- In a transactional local receiver demonstration, commit the key, business effect and result atomically under a uniqueness constraint.
- Simultaneous duplicate requests apply one effect; same key with different payload is rejected.
- Simulate a committed effect followed by lost acknowledgment; same-key retry returns the prior result without repeating the effect.
- Roll back effects and completion together on failure. Treat a missing record as potentially in-flight.
- Document retention, reconciliation and external receiver requirements. Never claim arbitrary exactly-once execution from a scheduler record alone.

Verification: `IdempotencyTest` covers concurrent duplicates, conflicting payloads, rollback, retained results, lost acknowledgments and three receiver crash boundaries. `./thredder retry-demo <receiver-file> <operation-key>` runs distinct scheduler jobs/attempts with one stable key. The receiver atomically replaces a flushed snapshot containing both balances and completion history; scheduler retry policy remains explicit and uncertain jobs are not automatically replayed. README records indefinite retention, reconciliation and external receiver requirements.

## v1.0 portfolio milestone — Measurement and explanation · Done

### T13: Benchmarks · Done

**Depends on:** T12.

- Compare worker counts, queue capacities, FIFO/aging policies and admission strategies for CPU-bound and blocking workloads.
- Record environment, warmup, repetitions, throughput, queue-wait percentiles, completion latency and rejection counts.
- Retain raw results and explain fairness/performance costs without unsupported claims.

### T14: Final README and interview walkthrough · Done

**Depends on:** T13. The final README and experiment-based portfolio walkthrough are complete.

- A fresh reader can build, test and run documented demos.
- Include reproduction, fix, evidence and tradeoffs for races, deadlocks, starvation, overload, shutdown and retries.
- Publish actual benchmark findings and keep planned/implemented distinctions accurate.
- Preserve project boundaries: backend CLI, indivisible jobs initially, no automatic splitting or exactly-once claims.

Verification: `benchmarks/v1.0-local` retains 128 trials (96 measured), environment metadata and 32,768 per-submission records across 32 configurations. `tools/summarize_benchmarks.py` validates raw accounting/percentiles and produces summaries. `benchmarks/README.md` explains measured tradeoffs and limits; `docs/WALKTHROUGH.md` maps every experiment to commands, tests and interview discussion.

## Cross-cutting follow-ups

Before treating Thredder as a long-running service, decide job-history retention, resource limits and observability. These are additional hardening considerations, not completed v0.1 guarantees. No frontend or distributed deployment is planned.

## 1.1 library preview · Implemented on plug-in branch

Depends on T14. Local preview; no Maven Central publication or framework adapter.

| Task | Status | Acceptance criteria |
|---|---|---|
| T15: Embeddable core and builder | Done | Core JAR excludes CLI/demo classes; existing CLI and tests still run; application owns shutdown budgets. |
| T16: Registered durable handlers | Done | Duplicate/unknown types reject; immutable registry; validation precedes admission/recovery mutations; custom queued jobs recover with original IDs. |
| T17: Real consumer example | Done | File-hasher compiles against only core, writes SHA-256 outputs, and resumes without resubmitting completed work. |
| T18: Integration guide and verification | Done | Document host responsibilities, lifecycle metrics, versioned handler contracts, packaging and limitations. |

Future choices: a Maven/Gradle publishing workflow and a host-specific adapter can follow a concrete consumer requirement. Dynamic plugin loading, sandboxing, hot reload and automatic retries are outside this preview.
