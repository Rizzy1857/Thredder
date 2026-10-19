package thredder;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ReceiverCrashFixture {
    private ReceiverCrashFixture() { }
    public static void main(String[] args) {
        AtomicBoolean armed = new AtomicBoolean();
        try (IdempotentReceiver receiver = new IdempotentReceiver(Path.of(args[0]), stage -> {
            if (armed.get() && stage.equals(args[1])) Runtime.getRuntime().halt(72);
        })) {
            armed.set(true);
            receiver.transfer(new IdempotentReceiver.Request("crash-key", "A", "B", 100));
        }
    }
}
