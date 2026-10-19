# v1.0 benchmark report

This is a small, reproducible scheduler experiment on one machine, not a production capacity estimate or a JMH microbenchmark. The retained run is [v1.0-local](v1.0-local/): [environment](v1.0-local/environment.txt), [all trials](v1.0-local/trials.csv), [all submissions](v1.0-local/samples.csv), and [summary](v1.0-local/summary.csv).

## Reproduce

```sh
./thredder benchmark benchmarks/my-run
python3 tools/summarize_benchmarks.py benchmarks/my-run
```

The output directory must not already exist; its parent must exist. The Java benchmark needs only the JDK. Python 3 is optional for the standard-library data validator and summary generator. The validator checks trial completeness, accepted/rejected accounting, per-job timings, percentile calculations and throughput against the retained raw data.

The recorded run used macOS 26.5.2 on aarch64, OpenJDK 27, 10 reported processors and a 4 GiB maximum heap. These describe this run, not required or recommended hardware. No external workload, disk persistence or network service was benchmarked.

## Method

The matrix has 32 configurations: CPU/blocking workload × 1/4 workers × capacity 8/64 × FIFO/aging × immediate/5-second admission. Each configuration has one warmup and three measured trials of 256 submissions from one producer. Configuration order is shuffled with seed 42; repetitions for a configuration run consecutively in one JVM. Priorities cycle through 0..10; aging adds one point per millisecond. The submitter cap is 16 and is not stressed by this single-producer experiment.

CPU jobs perform 200,000 iterations of integer arithmetic with a consumed result. This synthetic loop is subject to JIT optimization, and its very short jobs make scheduling, allocation, instrumentation and the shared result accumulator significant. Blocking jobs request a 2 ms sleep; OS scheduling determines actual sleep duration. Jobs are independent and no business side effect is involved.

Every accepted job must finish successfully or the run fails. Every rejected submission is recorded. Warmup data is retained but excluded from summaries. The summary reports medians of three trial-level metrics; its p95 is a median of trial p95s, not a pooled percentile. Use raw samples for other aggregations.

| Metric | Definition |
| --- | --- |
| Completed jobs/s | Accepted and successfully completed count divided by elapsed time from first submission through worker shutdown; construction excluded, drain included |
| Queue wait | Monotonic enqueue-to-dispatch interval captured inside the scheduler; excludes admission waiting and handler execution |
| Completion latency | Time before the admission call to the end of the handler; includes admission delay, queue delay and handler work, but not the final scheduler state update |
| Admission call | Time spent in the submission API, including rejection or waiting |
| Rejections | Rejected calls out of 256 attempts; no completion latency is assigned to rejected work |

Percentiles use nearest rank (p50, p95, p99). Queue timing is runtime telemetry, not part of the durable journal; restored completed jobs have no queue timing from a previous process. The benchmark uses no persistence, so these are not journal-commit latency numbers.

## Selected results

Each row is a median across three measured trials. Full results for all 32 configurations are in the summary CSV.

| Workload | Workers | Capacity | Policy | Offer ms | Completed/s | Rejected/256 | Queue p95 ms | Latency p95 ms |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Blocking | 1 | 8 | FIFO | 0 | 391.889 | 248 | 17.772 | 20.292 |
| Blocking | 1 | 8 | FIFO | 5000 | 398.164 | 0 | 20.260 | 25.289 |
| Blocking | 4 | 8 | FIFO | 5000 | 1600.777 | 0 | 5.096 | 10.033 |
| Blocking | 1 | 64 | FIFO | 5000 | 398.251 | 0 | 160.919 | 165.844 |
| Blocking | 4 | 8 | Aging | 5000 | 1589.647 | 0 | 10.085 | 14.843 |
| CPU | 1 | 8 | FIFO | 5000 | 51871.298 | 0 | 0.190 | 0.220 |
| CPU | 4 | 8 | FIFO | 5000 | 191658.656 | 0 | 0.042 | 0.065 |
| CPU | 4 | 8 | Aging | 5000 | 173163.101 | 0 | 0.205 | 0.223 |

## What the run supports

- More workers improved throughput for these independent workloads. For blocking jobs with capacity 8 and timed FIFO admission, throughput rose from about 398 to 1601 jobs/s when moving from one to four workers. This does not establish an optimal worker count for another machine or workload.
- Increasing capacity from 8 to 64 did not materially improve single-worker blocking throughput under timed admission (about 398 jobs/s in both cases), but queue p95 rose from about 20 to 161 ms. A bigger queue can store more delay without adding execution capacity.
- Immediate admission shed most of this burst. A low latency among the small accepted subset must not be interpreted as low latency for all 256 requested jobs. Timed admission admitted every job in this run but made the producer wait.
- Priority aging changes selection order. Its higher aggregate tail latency in the selected four-worker rows is consistent with some jobs waiting while others are promoted; scan overhead is another possible contributor. These measurements do not isolate either cause and do not prove starvation freedom. The fake-clock test supplies the explicit continuing-arrival fairness evidence.
- CPU trials are short and variable. Treat the numbers as demonstrations of the harness and workload-specific tradeoffs, not stable service limits or library comparisons.

## Limits and next measurements

Only three repetitions, one JVM, one producer and one local machine were used. There are no confidence intervals, isolated process forks, controlled CPU frequency, concurrent system-load control or long-duration steady-state trials. JIT compilation, garbage collection and configuration order can affect the result. Persistence, multi-producer contention, resource-lock contention and receiver snapshot costs were not measured. These are candidates for later work; none are implied by this v1.0 result.
