package thredder;

import java.util.concurrent.atomic.AtomicBoolean;

/** One-shot ownership token. Not a transaction, retry key, or recovery protocol. */
public final class AtomicClaim {
    private final AtomicBoolean claimed = new AtomicBoolean();

    public boolean tryClaim() { return claimed.compareAndSet(false, true); }
}
