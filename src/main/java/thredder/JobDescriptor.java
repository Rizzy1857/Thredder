package thredder;

import java.util.Objects;

/** Durable data only. The owning scheduler's registry validates and resolves the payload. */
public record JobDescriptor(String type, String argument) {
    public JobDescriptor {
        Objects.requireNonNull(type, "Job type required");
        Objects.requireNonNull(argument, "Job argument required");
        if (!type.matches("[A-Za-z][A-Za-z0-9._-]{0,127}"))
            throw new IllegalArgumentException("Job type must be a 1..128 character identifier");
    }
    /** Compatibility helper for the built-in sleep/fail types. Custom jobs use Scheduler.offer. */
    public Scheduler.Job handler() { return JobRegistry.defaults().resolve(this); }
}
