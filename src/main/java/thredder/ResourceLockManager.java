package thredder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/** One shared manager per resource domain; declare all resources in one call. */
public final class ResourceLockManager {
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final ThreadLocal<Boolean> inside = ThreadLocal.withInitial(() -> false);

    public void withLocks(Collection<String> resourceIds, Scheduler.Job action) throws Exception {
        if (resourceIds == null || action == null) throw new IllegalArgumentException("Resources and action required");
        TreeSet<String> ordered = new TreeSet<>();
        for (String id : resourceIds) {
            if (id == null || id.isBlank()) throw new IllegalArgumentException("Resource IDs must be nonblank");
            ordered.add(id);
        }
        if (inside.get()) throw new IllegalStateException("Nested resource locking is not supported; declare all IDs together");
        inside.set(true);
        var acquired = new ArrayList<ReentrantLock>();
        try {
            for (String id : ordered) {
                ReentrantLock lock = locks.computeIfAbsent(id, ignored -> new ReentrantLock());
                lock.lockInterruptibly();
                acquired.add(lock);
            }
            action.execute();
        } finally {
            for (int i = acquired.size() - 1; i >= 0; i--) acquired.get(i).unlock();
            inside.remove();
        }
    }
}
