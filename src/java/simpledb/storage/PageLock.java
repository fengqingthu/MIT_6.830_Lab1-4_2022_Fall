package simpledb.storage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * A helper class implementing a page-granular logical lock supporting
 * shared/exclusive levels. Locks are held in terms of transactions instead of
 * threads, although the implementation assumes a transaction is essentially one
 * thread.
 * 
 * This PageLock implements two seperate thread pools, where shared-lock
 * requests are prioritized as multiple transactions may progress, and a
 * queue-based lottery to avoid herd effect.
 */
public class PageLock {
    private final PageId pid;
    private TransactionId xHolder;
    private final Set<TransactionId> sHolder;
    private final Set<Ticket> sPool;
    private final List<Ticket> xQueue;
    private final DeadLockHandler dlHandler;

    public PageLock(PageId pid, DeadLockHandler dlHandler) {
        this.pid = pid;
        this.dlHandler = dlHandler;
        xHolder = null;
        sHolder = new HashSet<>();
        sPool = new HashSet<>();
        xQueue = new ArrayList<>();
    }

    public PageId getPid() {
        return pid;
    }

    private class Ticket {
        private final TransactionId tid;
        private final Condition cond;
        private final Lock lock;

        public Ticket(TransactionId tid, Condition cond, Lock lock) {
            this.tid = tid;
            this.cond = cond;
            this.lock = lock;
        }
    }

    /**
     * If the transaction already holds the sLock, this method simply returns
     * instead of throwing an exception. No downgrading ever incurred.
     */
    public void sLock(TransactionId tid) throws TransactionAbortedException {
        Lock lock = new ReentrantLock();
        Condition cond = lock.newCondition();
        Ticket ticket = new Ticket(tid, cond, lock);
        lock.lock();
        /*
         * Note(Qing): Here we explictly release the lock instead of using a finally
         * block to avoid IllegalMonitorStateException when running LockingTest in which
         * a thread waiting on the lock is forced to return and try to release the lock
         * it currently does not hold.
         */
        try {
            if (trySLock(tid)) {
                lock.unlock();
                return;
            }
            while (true) {
                synchronized (this) {
                    sPool.add(ticket);
                }
                dlHandler.waitFor(tid, this);
                cond.await();
                if (trySLock(tid)) {
                    dlHandler.unwait(tid, this);
                    lock.unlock();
                    return;
                }
            }
        } catch (InterruptedException e) {
            /* The deadlock handler may force this transaction to abort by interrupts. */
            // System.out.println(String.format("%d aborted.", tid.getId()));
            synchronized (this) {
                sPool.remove(ticket);
            }
            lock.unlock();
            throw new TransactionAbortedException();
        }
    }

    /**
     * If the transaction already holds the sLock, this method simply returns
     * instead of throwing an exception. No upgrading assumed.
     */
    public void xLock(TransactionId tid) throws TransactionAbortedException {
        Lock lock = new ReentrantLock();
        Condition cond = lock.newCondition();
        Ticket ticket = new Ticket(tid, cond, lock);
        lock.lock();
        try {
            if (tryXLock(tid)) {
                lock.unlock();
                return;
            }
            while (true) {
                synchronized (this) {
                    xQueue.add(ticket);
                }
                dlHandler.waitFor(tid, this);
                cond.await();
                if (tryXLock(tid)) {
                    dlHandler.unwait(tid, this);
                    lock.unlock();
                    return;
                }
            }
        } catch (InterruptedException e) {
            /* The deadlock handler may force this transaction to abort by interrupts. */
            // System.out.println(String.format("%d aborted.", tid.getId()));
            synchronized (this) {
                xQueue.remove(ticket);
            }
            lock.unlock();
            throw new TransactionAbortedException();
        }
    }

    public void sUnlock(TransactionId tid) {
        synchronized (this) {
            assert sHolder.contains(tid)
                    : String.format("transaction tries releasing a sLock it does not hold, tid: %s", tid.toString());
            sHolder.remove(tid);
        }
        lottery();
    }

    public void xUnlock(TransactionId tid) {
        synchronized (this) {
            assert tid == xHolder
                    : String.format("transaction tries releasing a xLock it does not hold, tid: %s", tid.toString());
            xHolder = null;
        }
        lottery();
    }

    /**
     * Release all locks on the page that the transaction holds and forfeit any
     * locks the transaction is waiting on, should be called upon aborts or commits.
     * 
     * @param tid TransactionId
     */
    public void releaseAll(TransactionId tid) {
        synchronized (this) {
            if (xHolder == tid)
                xHolder = null;
            if (sHolder.contains(tid))
                sHolder.remove(tid);
            xQueue.removeIf(t -> t.tid == tid);
            sPool.removeIf(t -> t.tid == tid);
            dlHandler.unwait(tid, this);
        }
        lottery();
    }

    public boolean holdsLock(TransactionId tid) {
        return holdsSLock(tid) || holdsXLock(tid);
    }

    public synchronized Set<TransactionId> getHolders() {
        if (!sHolder.isEmpty()) {
            return sHolder;
        }
        HashSet<TransactionId> res = new HashSet<>();
        if (xHolder != null) {
            res.add(xHolder);
            return res;
        }
        return res;
    }

    /* For test use. */
    public synchronized boolean holdsSLock(TransactionId tid) {
        return sHolder.contains(tid);
    }

    /* For test use. */
    public synchronized boolean holdsXLock(TransactionId tid) {
        return xHolder == tid;
    }

    /**
     * If the transaction already holds the xLock, it is granted the sLock but not
     * downgraded. If the transaction already holds the sLock, it returns true.
     */
    private synchronized boolean trySLock(TransactionId tid) {
        if (sHolder.contains(tid))
            return true;

        if (xHolder == null || xHolder == tid) {
            sHolder.add(tid);
            return true;
        }
        return false;
    }

    /**
     * If the transaction is the only one holds sLock, it can be upgraded to a
     * xLock. If the transaction already holds the xLock, it returns true.
     */
    private synchronized boolean tryXLock(TransactionId tid) {
        if (xHolder == tid)
            return true;

        if (xHolder == null && (sHolder.isEmpty() || (sHolder.size() == 1 && sHolder.contains(tid)))) {
            xHolder = tid;
            return true;
        }
        return false;
    }

    private synchronized void lottery() {
        if (xHolder == null) {
            /*
             * Note(Qing): We have all sLocks granted together but only notify one
             * single winner among xLock requests to avoid herd effect.
             * However, can xLock requests starve?
             */
            if (!sPool.isEmpty()) {
                for (Ticket t : sPool) {
                    t.lock.lock();
                    t.cond.signalAll();
                    t.lock.unlock();
                }
                sPool.clear();
            } else if (!xQueue.isEmpty()) {
                Ticket winner = xQueue.remove(0);
                winner.lock.lock();
                winner.cond.signalAll();
                winner.lock.unlock();
            }
        }
    }

}