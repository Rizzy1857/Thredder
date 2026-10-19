package thredder;

import java.nio.file.*;
import java.time.Duration;
import java.util.UUID;

/** Two distinct scheduler submissions retry one logical receiver operation. */
public final class RetryDemo {
    private RetryDemo() { }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Usage: ./thredder retry-demo <receiver-file> <operation-key>");
        Path store = Path.of(args[0]);
        var request = new IdempotentReceiver.Request(args[1], "A", "B", 100);
        try (IdempotentReceiver receiver = new IdempotentReceiver(store)) {
            UUID attempt1Id = UUID.randomUUID(), attempt2Id = UUID.randomUUID();
            Scheduler first = new Scheduler(1, 1);
            var attempt1 = first.submit("attempt 1: lose acknowledgment", () -> {
                receiver.transfer(request);
                throw new IllegalStateException("Simulated response lost AFTER receiver commit");
            });
            first.shutdown(Duration.ofSeconds(2), Duration.ofSeconds(1));
            Scheduler retry = new Scheduler(1, 1);
            var attempt2 = retry.submit("attempt 2: same operation key", () -> {
                var result = receiver.transfer(request);
                System.out.println("RECORDED RESULT " + result);
            });
            retry.shutdown(Duration.ofSeconds(2), Duration.ofSeconds(1));
            System.out.println("operationKey=" + request.key() + " attempt1Id=" + attempt1Id + " attempt1JobId=" + attempt1.id()
                + " attempt2Id=" + attempt2Id + " attempt2JobId=" + attempt2.id());
            System.out.println("attempt1=" + first.status(attempt1.id()).orElseThrow().state()
                + " attempt2=" + retry.status(attempt2.id()).orElseThrow().state());
            System.out.println("CURRENT BALANCES A=" + receiver.balance("A") + " B=" + receiver.balance("B"));
            if (retry.status(attempt2.id()).orElseThrow().state() != Scheduler.State.SUCCEEDED) throw new IllegalStateException("Retry failed");
        }
    }
}
