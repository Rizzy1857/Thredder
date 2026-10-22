package thredder;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/** Immutable application-owned bindings from durable type names to trusted handlers. */
public final class JobRegistry {
    @FunctionalInterface
    public interface Handler { void execute(String payload) throws Exception; }
    private record Binding(Consumer<String> validate, Handler handler) { }
    private final Map<String, Binding> bindings;
    private JobRegistry(Map<String, Binding> bindings) { this.bindings = Map.copyOf(bindings); }
    public static Builder builder() { return new Builder(); }
    public static JobRegistry defaults() { return builder().builtIns().build(); }
    public Set<String> types() { return bindings.keySet(); }

    Scheduler.Job resolve(JobDescriptor descriptor) {
        Binding binding = bindings.get(descriptor.type());
        if (binding == null) throw new IllegalArgumentException("Unregistered job type: " + descriptor.type());
        binding.validate().accept(descriptor.argument());
        return () -> binding.handler().execute(descriptor.argument());
    }

    public static final class Builder {
        private final Map<String, Binding> bindings = new LinkedHashMap<>();
        private Builder() { }
        /** Validators run before admission and during recovery; they must be fast and side-effect-free. */
        public Builder register(String type, Consumer<String> validate, Handler handler) {
            new JobDescriptor(type, ""); // Validate the type identifier, not an application payload.
            if (bindings.containsKey(type)) throw new IllegalArgumentException("Duplicate job type: " + type);
            bindings.put(type, new Binding(Objects.requireNonNull(validate), Objects.requireNonNull(handler)));
            return this;
        }
        public Builder builtIns() {
            register("sleep", payload -> {
                long millis = Long.parseLong(payload);
                if (millis < 0 || millis > 60_000) throw new IllegalArgumentException("Sleep must be 0..60000 ms");
            }, payload -> Thread.sleep(Long.parseLong(payload)));
            register("fail", payload -> {
                if (payload.isBlank()) throw new IllegalArgumentException("Failure message required");
            }, payload -> { throw new IllegalStateException(payload); });
            return this;
        }
        public JobRegistry build() { return new JobRegistry(bindings); }
    }
}
