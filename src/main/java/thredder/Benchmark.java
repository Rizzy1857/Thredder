package thredder;

import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** Small reproducible end-to-end experiment, not a microbenchmark or capacity claim. */
public final class Benchmark {
    private Benchmark() { }
    private record Config(String workload, int workers, int capacity, Scheduler.Policy policy, int offerMs) { }
    private static final int JOBS = 256, CPU_ITERATIONS = 200_000;
    private static final AtomicLong BLACKHOLE = new AtomicLong();

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Usage: ./thredder benchmark <new-output-directory>");
        Path directory = Path.of(args[0]);
        Files.createDirectory(directory); // Never replace an earlier run's evidence.
        Files.writeString(directory.resolve("environment.txt"),
            "timestamp=" + Instant.now() + "\njava=" + System.getProperty("java.runtime.version")
            + "\nvm=" + System.getProperty("java.vm.name") + "\nos=" + System.getProperty("os.name")
            + " " + System.getProperty("os.version") + "\narch=" + System.getProperty("os.arch")
            + "\navailableProcessors=" + Runtime.getRuntime().availableProcessors()
            + "\nmaxHeapBytes=" + Runtime.getRuntime().maxMemory()
            + "\njobsPerTrial=" + JOBS + "\ncpuIterations=" + CPU_ITERATIONS
            + "\nblockingMillis=2\nproducerThreads=1\nsubmitterCap=16\nagingMillis=1\nseed=42"
            + "\nwarmupTrialsPerConfig=1\nmeasuredTrialsPerConfig=3\npersistence=false\n");
        List<Config> configs = new ArrayList<>();
        for (String workload : List.of("cpu", "blocking"))
            for (int workers : new int[]{1, 4}) for (int capacity : new int[]{8, 64})
                for (Scheduler.Policy policy : Scheduler.Policy.values()) for (int offer : new int[]{0, 5000})
                    configs.add(new Config(workload, workers, capacity, policy, offer));
        Collections.shuffle(configs, new Random(42));
        try (PrintWriter trials = new PrintWriter(Files.newBufferedWriter(directory.resolve("trials.csv")));
             PrintWriter samples = new PrintWriter(Files.newBufferedWriter(directory.resolve("samples.csv")))) {
            trials.println("trial,phase,workload,workers,capacity,policy,offer_ms,submitted,accepted,rejected,elapsed_ms,completed_per_s,queue_p50_ms,queue_p95_ms,queue_p99_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms");
            samples.println("trial,phase,job_index,priority,accepted,queue_wait_ns,submit_to_completion_ns,admission_call_ns");
            int trial = 0;
            for (Config config : configs) {
                for (int repeat = 0; repeat < 4; repeat++) {
                    String phase = repeat == 0 ? "warmup" : "measured";
                    run(++trial, phase, config, trials, samples);
                }
                System.out.println("Measured " + config);
            }
            if (trials.checkError() || samples.checkError()) throw new IOException("Benchmark output failed");
        }
        System.out.println("Saved 128 trials (96 measured) and per-submission samples to " + directory);
    }
    private static void run(int trial, String phase, Config config, PrintWriter trials, PrintWriter samples) throws Exception {
        Scheduler scheduler = new Scheduler(config.workers(), config.capacity(), config.policy(), Duration.ofMillis(1), System::nanoTime);
        long[] submitted = new long[JOBS], ended = new long[JOBS], admission = new long[JOBS];
        UUID[] ids = new UUID[JOBS];
        long start = System.nanoTime();
        try {
            for (int i = 0; i < JOBS; i++) {
                final int index = i;
                submitted[i] = System.nanoTime();
                var result = scheduler.offer("bench " + i, i % 11, () -> {
                    if (config.workload().equals("blocking")) Thread.sleep(2);
                    else {
                        long x = index + 1;
                        for (int n = 0; n < CPU_ITERATIONS; n++) x = x * 2862933555777941757L + 3037000493L;
                        BLACKHOLE.addAndGet(x);
                    }
                    ended[index] = System.nanoTime();
                }, Duration.ofMillis(config.offerMs()));
                admission[i] = System.nanoTime() - submitted[i]; ids[i] = result.id();
            }
            var report = scheduler.shutdown(Duration.ofSeconds(10), Duration.ofSeconds(1));
            if (!report.terminated() || !report.unresolved().isEmpty()) throw new IllegalStateException("Benchmark did not drain");
            long elapsed = System.nanoTime() - start;
            List<Long> waits = new ArrayList<>(), latencies = new ArrayList<>();
            for (int i = 0; i < JOBS; i++) {
                Long wait = null, latency = null;
                if (ids[i] != null) {
                    var status = scheduler.status(ids[i]).orElseThrow();
                    if (status.state() != Scheduler.State.SUCCEEDED || ended[i] == 0 || status.queueWaitNanos() == null)
                        throw new IllegalStateException("Invalid benchmark job outcome");
                    wait = status.queueWaitNanos(); latency = ended[i] - submitted[i];
                    waits.add(wait); latencies.add(latency);
                }
                samples.printf(Locale.ROOT, "%d,%s,%d,%d,%s,%s,%s,%d%n", trial, phase, i, i % 11,
                    ids[i] != null, wait == null ? "" : wait, latency == null ? "" : latency, admission[i]);
            }
            Collections.sort(waits); Collections.sort(latencies);
            trials.printf(Locale.ROOT, "%d,%s,%s,%d,%d,%s,%d,%d,%d,%d,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f%n",
                trial, phase, config.workload(), config.workers(), config.capacity(), config.policy(), config.offerMs(), JOBS,
                waits.size(), JOBS - waits.size(), elapsed / 1e6, waits.size() * 1e9 / elapsed,
                percentile(waits, .5), percentile(waits, .95), percentile(waits, .99),
                percentile(latencies, .5), percentile(latencies, .95), percentile(latencies, .99));
        } finally { scheduler.close(); }
    }
    private static double percentile(List<Long> sorted, double fraction) {
        return sorted.isEmpty() ? Double.NaN : sorted.get(Math.max(0, (int) Math.ceil(sorted.size() * fraction) - 1)) / 1e6;
    }
}
