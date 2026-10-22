package thredder;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

final class LibraryTest {
    private LibraryTest() { }
    static void check(boolean value, String message) { ConcurrencyTest.check(value, message); }
    static void run() throws Exception {
        registryContract();
        durableCustomJob();
        failedRecoveryPreflight();
        configuredClose();
        System.out.println("PASS: library registry, custom durable recovery, preflight safety, builder lifecycle and metrics");
    }
    static void registryContract() throws Exception {
        AtomicInteger total = new AtomicInteger();
        var builder = JobRegistry.builder().register("count.v1", payload -> {
            if (Integer.parseInt(payload) < 0) throw new IllegalArgumentException("negative count");
        }, payload -> total.addAndGet(Integer.parseInt(payload)));
        JobRegistry first = builder.build();
        builder.register("later.v1", payload -> {}, payload -> {});
        check(!first.types().contains("later.v1"), "registry immutable after build");
        boolean duplicate = false;
        try { builder.register("count.v1", payload -> {}, payload -> {}); }
        catch (IllegalArgumentException expected) { duplicate = true; }
        check(duplicate, "duplicate type rejected");
        try (Scheduler scheduler = Scheduler.builder().registry(first).workers(1).capacity(2).build()) {
            for (JobDescriptor descriptor : List.of(new JobDescriptor("unknown", "1"), new JobDescriptor("count.v1", "-1"))) {
                boolean rejected = false;
                try { scheduler.offer("bad", 0, descriptor, Duration.ZERO); }
                catch (IllegalArgumentException expected) { rejected = true; }
                check(rejected && scheduler.list().isEmpty(), "invalid/unregistered payload not admitted");
            }
            var submitted = scheduler.offer("count", 0, new JobDescriptor("count.v1", "7"), Duration.ZERO);
            check(submitted.accepted(), "custom job admitted");
        }
        check(total.get() == 7, "host handler called with exact payload");
    }
    static void durableCustomJob() throws Exception {
        Path store = Files.createTempDirectory(Path.of("build"), "library-").resolve("jobs.thr");
        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        JobRegistry registry = JobRegistry.builder()
            .register("block.v1", payload -> {}, payload -> { started.countDown(); new CountDownLatch(1).await(); })
            .register("count.v1", Integer::parseInt, payload -> calls.addAndGet(Integer.parseInt(payload))).build();
        Scheduler scheduler = Scheduler.builder().workers(1).capacity(2).store(store).registry(registry).build();
        UUID queued;
        try {
            scheduler.offer("block", 0, new JobDescriptor("block.v1", ""), Duration.ZERO);
            ConcurrencyTest.await(started);
            queued = scheduler.offer("count", 0, new JobDescriptor("count.v1", "9"), Duration.ZERO).id();
            var metrics = scheduler.metrics();
            check(metrics.queued() == 1 && metrics.running() == 1 && metrics.activeWorkers() == 1, "coherent live metrics");
            check(scheduler.shutdown(Duration.ZERO, Duration.ofSeconds(1)).pending().equals(List.of(queued)), "custom queued work preserved");
        } finally { scheduler.shutdown(Duration.ZERO, Duration.ofSeconds(1)); }
        Scheduler recovered = Scheduler.builder().workers(1).capacity(2).registry(registry).store(store).build();
        recovered.close();
        check(calls.get() == 9 && recovered.status(queued).orElseThrow().state() == Scheduler.State.SUCCEEDED, "custom payload and ID restored");
        var metrics = recovered.metrics();
        check(metrics.succeeded() == 1 && metrics.cancelled() == 1 && metrics.activeWorkers() == 0 && !metrics.accepting(), "terminal metrics");
        try (Scheduler again = Scheduler.builder().registry(registry).store(store).build()) {
            check(again.metrics().succeeded() == 1, "terminal history not re-executed");
        }
        check(calls.get() == 9, "restart does not duplicate completed custom job");
    }
    static void failedRecoveryPreflight() throws Exception {
        Path store = Files.createTempDirectory(Path.of("build"), "registry-preflight-").resolve("jobs.thr");
        try (Journal journal = new Journal(store, Journal.Probe.NONE)) {
            for (String type : List.of("count.v1", "missing.v1")) journal.append(new Journal.Record(UUID.randomUUID(), type,
                new JobDescriptor(type, "1"), 0, Scheduler.State.QUEUED, Instant.now(), null, null, null));
        }
        byte[] bytes = Files.readAllBytes(store);
        AtomicInteger calls = new AtomicInteger();
        JobRegistry registry = JobRegistry.builder().register("count.v1", Integer::parseInt, payload -> calls.incrementAndGet()).build();
        boolean rejected = false;
        try (Scheduler scheduler = Scheduler.builder().registry(registry).store(store).build()) { scheduler.list(); }
        catch (IllegalArgumentException expected) { rejected = true; }
        check(rejected && calls.get() == 0 && Arrays.equals(bytes, Files.readAllBytes(store)), "unknown recovered type fails before executing or rewriting any job");
        JobRegistry repaired = JobRegistry.builder().register("count.v1", Integer::parseInt, payload -> calls.incrementAndGet())
            .register("missing.v1", Integer::parseInt, payload -> calls.incrementAndGet()).build();
        try (Scheduler scheduler = Scheduler.builder().registry(repaired).store(store).build()) { scheduler.list(); }
        check(calls.get() == 2, "failed constructor released store ownership");
    }
    static void configuredClose() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        Scheduler scheduler = Scheduler.builder().workers(1).shutdownTimeouts(Duration.ZERO, Duration.ofSeconds(1)).build();
        var job = scheduler.submit("interrupt", () -> { started.countDown(); new CountDownLatch(1).await(); });
        ConcurrencyTest.await(started);
        scheduler.close();
        check(scheduler.status(job.id()).orElseThrow().state() == Scheduler.State.CANCELLED, "builder close budgets honored");
    }
}
