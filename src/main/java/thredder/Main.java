package thredder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.UUID;
import java.time.Duration;
import java.nio.file.Path;

public final class Main {
    private Main() { }

    public static void main(String[] args) throws IOException {
        int workers = 2;
        int capacity = 16;
        Scheduler.Policy policy = Scheduler.Policy.FIFO;
        int agingMillis = 1000;
        int submitters = 16;
        int offerMillis = 0;
        int shutdownMillis = 5000;
        int cancelMillis = 1000;
        Path store = null;
        try {
            for (int i = 0; i < args.length; i += 2) {
                if (i + 1 >= args.length) throw new IllegalArgumentException("Missing option value");
                if (args[i].equals("--store")) {
                    store = Path.of(args[i + 1]);
                    continue;
                }
                if (args[i].equals("--policy")) {
                    policy = switch (args[i + 1]) {
                        case "fifo" -> Scheduler.Policy.FIFO;
                        case "aging" -> Scheduler.Policy.PRIORITY_AGING;
                        default -> throw new IllegalArgumentException("Policy must be fifo or aging");
                    };
                    continue;
                }
                int value = Integer.parseInt(args[i + 1]);
                if (value < 0 || (value == 0 && !args[i].equals("--offer-ms") && !args[i].equals("--shutdown-ms") && !args[i].equals("--cancel-ms")))
                    throw new IllegalArgumentException("Options must be positive; offer/shutdown/cancel timeouts may be zero");
                switch (args[i]) {
                    case "--workers" -> workers = value;
                    case "--capacity" -> capacity = value;
                    case "--aging-ms" -> agingMillis = value;
                    case "--submitters" -> submitters = value;
                    case "--offer-ms" -> offerMillis = value;
                    case "--shutdown-ms" -> shutdownMillis = value;
                    case "--cancel-ms" -> cancelMillis = value;
                    default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
                }
            }
        } catch (IllegalArgumentException ex) {
            System.err.println("ERROR " + ex.getMessage());
            System.err.println("Usage: ./thredder run [--workers N] [--capacity N] [--policy fifo|aging] [--aging-ms N] [--submitters N] [--offer-ms N] [--store PATH] [--shutdown-ms N] [--cancel-ms N]");
            System.exit(2);
            return;
        }
        final Scheduler scheduler;
        try {
            scheduler = new Scheduler(workers, capacity, policy, Duration.ofMillis(agingMillis), System::nanoTime, submitters, store);
        } catch (RuntimeException failure) {
            System.err.println("ERROR Cannot open scheduler: " + failure);
            System.exit(1); return;
        }
        Scheduler.ShutdownReport report;
        try {
            run(new BufferedReader(new InputStreamReader(System.in)), new PrintWriter(System.out, true), scheduler, Duration.ofMillis(offerMillis));
        } catch (RuntimeException failure) {
            System.err.println("ERROR Scheduler failed: " + failure);
        } finally {
            report = scheduler.shutdown(Duration.ofMillis(shutdownMillis), Duration.ofMillis(cancelMillis));
            System.out.println("SHUTDOWN terminated=" + report.terminated() + " pending=" + report.pending()
                + " unresolved=" + report.unresolved() + " storageError=" + report.storageError());
        }
        if (!report.terminated() || !report.unresolved().isEmpty() || report.storageError() != null) System.exit(3);
    }

    static void run(BufferedReader input, PrintWriter output, Scheduler scheduler) throws IOException {
        run(input, output, scheduler, Duration.ZERO);
    }

    static void run(BufferedReader input, PrintWriter output, Scheduler scheduler, Duration offerTimeout) throws IOException {
        output.println("Thredder v1.1.0-SNAPSHOT — type help for commands");
        String line;
        while ((line = input.readLine()) != null) {
            String stripped = line.strip();
            if (stripped.isEmpty()) continue;
            String[] parts = stripped.split("\\s+", 3);
            try {
                switch (parts[0]) {
                    case "help" -> {
                        require(parts.length == 1, "help takes no arguments");
                        output.println("submit sleep <milliseconds> | submit fail <message> | priority <0..10> <sleep|fail> <argument> | list | status <UUID> | help | exit");
                    }
                    case "submit", "priority" -> {
                        int priority = 0;
                        if (parts[0].equals("priority")) {
                            require(parts.length == 3, "Use priority <0..10> <sleep|fail> <argument>");
                            priority = Integer.parseInt(parts[1]);
                            String[] jobParts = parts[2].split("\\s+", 2);
                            require(jobParts.length == 2, "Job type and argument required");
                            parts = new String[]{"submit", jobParts[0], jobParts[1]};
                        }
                        require(parts.length == 3, "Use submit sleep <milliseconds> or submit fail <message>");
                        JobDescriptor descriptor = new JobDescriptor(parts[1], parts[2]);
                        Scheduler.Submission result = scheduler.offer(parts[1] + " " + parts[2], priority, descriptor, offerTimeout);
                        output.println(result.accepted() ? "ACCEPTED " + result.id() : "REJECTED " + result.rejection());
                    }
                    case "list" -> {
                        require(parts.length == 1, "list takes no arguments");
                        var jobs = scheduler.list();
                        if (jobs.isEmpty()) output.println("No jobs");
                        jobs.forEach(job -> show(output, job));
                    }
                    case "status" -> {
                        require(parts.length == 2, "Use status <UUID>");
                        String requestedId = parts[1];
                        scheduler.status(UUID.fromString(requestedId)).ifPresentOrElse(
                            job -> show(output, job), () -> output.println("NOT FOUND " + requestedId));
                    }
                    case "exit" -> {
                        require(parts.length == 1, "exit takes no arguments");
                        output.println("Draining accepted jobs within the shutdown deadline...");
                        return;
                    }
                    default -> output.println("ERROR Unknown command. Type help.");
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                output.println("REJECTED Admission interrupted; exiting");
                return;
            } catch (IllegalArgumentException ex) {
                output.println("ERROR " + ex.getMessage());
            }
        }
        output.println("End of input; starting shutdown...");
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }

    private static void show(PrintWriter output, Scheduler.Snapshot job) {
        output.println(job.id() + " " + job.state() + " priority=" + job.priority() + " " + job.name()
            + (job.error() == null ? "" : " | " + job.error()));
    }
}
