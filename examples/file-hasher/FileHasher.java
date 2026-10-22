package examples;

import thredder.JobDescriptor;
import thredder.JobRegistry;
import thredder.Scheduler;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;

/** Example host application; depends only on thredder-core.jar. */
public final class FileHasher {
    private FileHasher() { }
    public static void main(String[] args) throws Exception {
        if (args.length != 4 || !(args[3].equals("submit") || args[3].equals("resume")))
            throw new IllegalArgumentException("Usage: FileHasher <input-dir> <output-dir> <journal> submit|resume");
        Path input = Path.of(args[0]).toRealPath();
        Files.createDirectories(Path.of(args[1]));
        Path output = Path.of(args[1]).toRealPath();
        if (output.startsWith(input) || input.startsWith(output))
            throw new IllegalArgumentException("Use separate, non-nested input and output directories");
        JobRegistry registry = JobRegistry.builder().register("files.sha256.v1", payload -> {
            Path relative = Path.of(payload);
            if (relative.isAbsolute() || relative.getNameCount() != 1 || payload.equals(".") || payload.equals(".."))
                throw new IllegalArgumentException("Payload must be one relative filename");
        }, payload -> hash(input, output, payload)).build();
        Scheduler scheduler = Scheduler.builder().workers(2).capacity(64).submitterLimit(4)
            .registry(registry).store(Path.of(args[2])).build();
        Scheduler.ShutdownReport report;
        try {
            if (args[3].equals("submit")) {
                List<Path> paths;
                try (var stream = Files.list(input)) {
                    paths = stream.filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)).sorted().toList();
                }
                for (Path path : paths) {
                    var result = scheduler.offer("Hash " + path.getFileName(), 0,
                        new JobDescriptor("files.sha256.v1", path.getFileName().toString()), Duration.ofSeconds(5));
                    if (!result.accepted()) throw new IllegalStateException(result.rejection());
                    System.out.println("ACCEPTED " + result.id() + " " + path.getFileName());
                }
            }
        } finally {
            report = scheduler.shutdown(Duration.ofSeconds(30), Duration.ofSeconds(2));
            System.out.println(scheduler.metrics());
            System.out.println(report);
        }
        if (!report.terminated() || !report.unresolved().isEmpty() || report.storageError() != null
                || scheduler.metrics().failed() > 0 || !report.pending().isEmpty())
            throw new IllegalStateException("Some work needs inspection; use resume for pending work, not automatic retry of unresolved work");
    }
    private static void hash(Path input, Path output, String name) throws Exception {
        Path file = input.resolve(name);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        // NOFOLLOW_LINKS rejects symbolic-link inputs at open time.
        try (InputStream stream = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            byte[] buffer = new byte[64 * 1024];
            int count;
            while ((count = stream.read(buffer)) != -1) {
                if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Hashing cancelled");
                digest.update(buffer, 0, count);
            }
        }
        Path temporary = Files.createTempFile(output, ".sha256-", ".pending");
        try {
            Files.writeString(temporary, HexFormat.of().formatHex(digest.digest()) + "\n", StandardCharsets.UTF_8);
            Files.move(temporary, output.resolve(name + ".sha256"), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally { Files.deleteIfExists(temporary); }
    }
}
