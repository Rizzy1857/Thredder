package thredder;

import java.util.Objects;

/** Allowlisted, version-independent job data; never serialized executable objects. */
public record JobDescriptor(String type, String argument) {
    public JobDescriptor {
        Objects.requireNonNull(type, "Job type required");
        Objects.requireNonNull(argument, "Job argument required");
        switch (type) {
            case "sleep" -> {
                long millis = Long.parseLong(argument);
                if (millis < 0 || millis > 60_000) throw new IllegalArgumentException("Sleep must be 0..60000 ms");
            }
            case "fail" -> {
                if (argument.isBlank()) throw new IllegalArgumentException("Failure message required");
            }
            default -> throw new IllegalArgumentException("Unknown job type: " + type);
        }
    }
    public Scheduler.Job handler() {
        return switch (type) {
            case "sleep" -> () -> Thread.sleep(Long.parseLong(argument));
            case "fail" -> () -> { throw new IllegalStateException(argument); };
            default -> throw new IllegalStateException("Unvalidated descriptor");
        };
    }
}
