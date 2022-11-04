package simpledb.storage;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * A helper class to detect and handle deadlocks.
 */
public class LockManager {
    private final ConcurrentHashMap<TransactionId, Set<PageLock>> lockMap;

    public LockManager() {
        lockMap = new ConcurrentHashMap<>();
    }

    public void grabLock(TransactionId tid, Page page, Permissions perm) throws TransactionAbortedException {
        if (!lockMap.containsKey(tid)) {
            lockMap.put(tid, new HashSet<PageLock>());
        }
        PageLock lock = page.getPgLock();
        if (perm == Permissions.READ_ONLY) {
            lock.sLock(tid);
        }
        if (perm == Permissions.READ_WRITE) {
            lock.xLock(tid);
        }
        lockMap.get(tid).add(lock);
    }

    public void unsafeRelease(TransactionId tid, Page page) {
        PageLock lock = page.getPgLock();
        Database.getBufferPool().getDLHandler().unwait(tid, lock);
        lock.releaseAll(tid);
        if (lockMap.containsKey(tid))
            lockMap.get(tid).remove(lock);
    }

    public boolean isLocked(Page page) {
        return lockMap.contains(page.getPgLock());
    }

    public void releaseAll(TransactionId tid) {
        Database.getBufferPool().getDLHandler().unwaitAll(tid);
        if (lockMap.containsKey(tid)) {
            for (PageLock lock: lockMap.get(tid)) {
                lock.releaseAll(tid);
            }
            lockMap.remove(tid);
        }
    }

}