# Embedding Thredder

Thredder 1.1.0-SNAPSHOT is a dependency-free Java library preview. Use it inside a Java application that needs a bounded queue and a fixed number of concurrent background workers: local file processing, batch transformations or other finite tasks. The host registers trusted handlers and owns scheduler startup and shutdown. No frontend, network service, automatic job splitting or host-specific plugin installation is provided.

## Package and run

Requires Bash and JDK 17+; compilation targets Java 17. Verification used OpenJDK 27, not a Java 17 runtime.

```sh
./thredder test
./thredder package
java -jar build/libs/thredder-cli-1.1.0-SNAPSHOT.jar
```

Use your configured JDK's `java` if it is not on PATH. Keep the core JAR next to the CLI/example JAR: their manifests refer to its relative filename. An application needs only `build/libs/thredder-core-1.1.0-SNAPSHOT.jar` on its compile and runtime classpath. Packaging compiles the CLI and example against that JAR, without access to core source files. No Maven repository upload or stable 1.1 release is implied.

## Register application work

```java
import thredder.JobDescriptor;
import thredder.JobRegistry;
import thredder.Scheduler;
import java.nio.file.Path;
import java.time.Duration;

class Example {
    public static void main(String[] args) throws Exception {
        JobRegistry handlers = JobRegistry.builder()
            .register("message.print.v1", payload -> {
                if (payload.isBlank()) throw new IllegalArgumentException("Message required");
            }, payload -> System.out.println(payload))
            .build();
        try (Scheduler scheduler = Scheduler.builder()
                .workers(2).capacity(16).submitterLimit(4)
                .registry(handlers).store(Path.of("data/messages.thr"))
                .shutdownTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(1))
                .build()) {
            var submitted = scheduler.offer("Print message", 0,
                new JobDescriptor("message.print.v1", "Hello"), Duration.ofSeconds(5));
            if (!submitted.accepted()) throw new IllegalStateException(submitted.rejection());
            System.out.println(submitted.id());
        }
    }
}
```

Printing is illustrative; a crash after output but before completion remains uncertain. Existing constructors retain built-in `sleep` and `fail` handlers. A custom registry replaces the defaults; call `.builtIns()` before `.register(...)` to include them. Descriptor construction checks the type identifier; payload validation occurs when offered to a scheduler and during recovery. The `JobDescriptor.handler()` compatibility helper resolves only built-ins.

Registries are immutable snapshots. Duplicate type names fail registration. Validation must be fast, deterministic and side-effect-free: it runs on producer/startup threads before admission permits, so do not perform I/O there. The submitter cap bounds scheduler admission, not work callers perform beforehand. Handler execution can occur concurrently; shared handler state must be thread-safe. Cooperate with interruption and release resources in `finally` blocks. These are trusted Java callbacks, with no security sandbox, classloader isolation or hot reload.

## Persistence and lifecycle contracts

Use versioned types such as `files.sha256.v1` when payload meaning changes. Persisted descriptors contain a type and string payload, not Java objects or handler code. Keep bindings and validators compatible with all retained records, including completed history. Startup validates every record before recovery changes or worker startup; missing types or invalid payloads fail startup and release the journal lock. Recovery requires enough queue capacity for preserved queued work.

Only QUEUED work resumes. A previously RUNNING job becomes UNRESOLVED and requires application reconciliation. Job IDs identify submissions, not idempotent business operations. Exactly-once execution is not guaranteed. External effects need receiver-owned idempotency keys and atomic effect/completion records; the separate retry demo explains that contract.

The host must keep its process alive and close the scheduler. Workers are daemon threads. `close()` uses builder shutdown budgets; call `shutdown(drain, observeCancellation)` explicitly when you need its report of pending, unresolved and still-live work. An interrupt is a request, not forced termination. Persistent pending jobs remain recoverable; an uncooperative handler may remain alive after a deadline.

`metrics()` reports current lifecycle counts, executing handlers, active submitters, admission state and storage errors. Terminal counts include retained history; this is not a rolling throughput metric. Queue capacity bounds pending jobs, not history or journal size. History retention, journal compaction and remote monitoring remain future work.

## File-hashing consumer

The example scans immediate regular files, skips symlinks, and schedules one indivisible SHA-256 job per file. Each result is atomically replaced as `<filename>.sha256` in a separate output directory.

```sh
mkdir -p data/input data/checksums
printf 'hello\n' > data/input/hello.txt
java -jar build/libs/thredder-file-hasher-1.1.0-SNAPSHOT.jar \
  data/input data/checksums data/hashes.thr submit
cat data/checksums/hello.txt.sha256
java -jar build/libs/thredder-file-hasher-1.1.0-SNAPSHOT.jar \
  data/input data/checksums data/hashes.thr resume
```

Use the same input/output roots when resuming and keep input files unchanged while processing. `resume` runs preserved queued jobs without creating new submissions; `submit` intentionally creates new jobs even for previously processed files. The example has two workers, queue capacity 64, a five-second offer budget and a 30-second drain budget. Inspect reported failures/unresolved work; it does not automatically retry them. Completed failures remain visible in retained history.

Atomic output replacement requires filesystem support and is not a transaction with the scheduler journal or a power-loss durability guarantee. This example is intended for trusted local directories; it does not snapshot files or defend against hostile concurrent filesystem changes. General-purpose distributed scheduling and transactional business processing remain outside its scope.
