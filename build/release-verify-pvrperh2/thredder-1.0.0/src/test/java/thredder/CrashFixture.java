package thredder;

import java.nio.file.*;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/** Invoked only by RecoveryTest in a disposable child JVM. */
public final class CrashFixture {
    private CrashFixture() { }
    public static void main(String[] args) throws Exception {
        Path store = Path.of(args[0]);
        String boundary = args[1];
        Journal.Probe probe = (stage, record) -> {
            try { Files.writeString(Path.of(args[0] + ".id"), record.id().toString()); }
            catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
            if (boundary.equals("torn-accept") && stage.equals("before-write") && record.state() == Scheduler.State.QUEUED) {
                try {
                    Files.write(store, new byte[]{0x54, 0x48, 0x52, 0x34, 0}, StandardOpenOption.APPEND);
                    try (var channel = java.nio.channels.FileChannel.open(store, StandardOpenOption.WRITE)) { channel.force(true); }
                } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
                Runtime.getRuntime().halt(71);
            }
            if ((stage + ":" + record.state()).equals(boundary)) Runtime.getRuntime().halt(71);
        };
        Scheduler scheduler = new Scheduler(1, 2, Scheduler.Policy.FIFO, Duration.ofSeconds(1), System::nanoTime, 2, store, probe);
        scheduler.offer("crash job", 0, new JobDescriptor("sleep", "0"), Duration.ZERO);
        new CountDownLatch(1).await();
    }
}
