package thredder;

import java.util.List;

/** Two-resource learning example; not a financial transaction system. */
public final class TransferDemo {
    private TransferDemo() { }

    public static void main(String[] args) {
        ResourceLockManager locks = new ResourceLockManager();
        int[] balances = {10_000, 10_000};
        Scheduler scheduler = new Scheduler(4, 200);
        try {
            for (int i = 0; i < 100; i++) {
                int from = i % 2;
                int to = 1 - from;
                var result = scheduler.submit("transfer " + from + " to " + to, () ->
                    locks.withLocks(List.of("account-" + from, "account-" + to), () -> {
                        if (balances[from] < 1) throw new IllegalStateException("Insufficient balance");
                        balances[from]--;
                        balances[to]++;
                    }));
                if (!result.accepted()) throw new IllegalStateException(result.rejection());
            }
        } finally { scheduler.close(); }
        if (scheduler.list().stream().anyMatch(job -> job.state() != Scheduler.State.SUCCEEDED)
                || balances[0] != 10_000 || balances[1] != 10_000) {
            throw new AssertionError("Transfer invariant failed");
        }
        System.out.println("100 opposing transfers completed; balances = " + balances[0] + ", " + balances[1]);
    }
}
