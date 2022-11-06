package simpledb.storage;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    public PageLock(PageId pid) {
        this.pid = pid;
        xHolder = null;
        sHolder = new HashSet<>();
        sPool = new HashSet<>();
        xQueue = new LinkedList<>();
    }

    public PageId getPid() {
        return pid;
    }

    private class Ticket {
        private final TransactionId tid;
        private final Condition cond;

        public Ticket(TransactionId tid, Condition cond) {
            this.tid = tid;
            this.cond = cond;
        }
    }

    /**
     * If the transaction already holds the sLock, this method simply returns
     * instead of throwing an exception. No downgrading ever incurred.
     */
    public void sLock(TransactionId tid) {
        Lock lock = new ReentrantLock();
        Condition cond = lock.newCondition();
        Ticket ticket = new Ticket(tid, cond);
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
                sPool.add(ticket);
                cond.await();
                if (trySLock(tid)) {
                    lock.unlock();
                    return;
                }
            }
        } catch (InterruptedException e) {
            /* If the thread is interrupted, simply retry. */
            sPool.remove(ticket);
            lock.unlock();
            sLock(tid);
        }
    }

    /**
     * If the transaction already holds the sLock, this method simply returns
     * instead of throwing an exception. No upgrading assumed.
     */
    public void xLock(TransactionId tid) {
        Lock lock = new ReentrantLock();
        Condition cond = lock.newCondition();
        Ticket ticket = new Ticket(tid, cond);
        lock.lock();
        try {
            if (tryXLock(tid)) {
                lock.unlock();
                return;
            }
            while (true) {
                xQueue.add(ticket);
                cond.await();
                if (tryXLock(tid)) {
                    lock.unlock();
                    return;
                }
            }
        } catch (InterruptedException e) {
            /* If the thread is interrupted, simply retry. */
            xQueue.remove(ticket);
            lock.unlock();
            xLock(tid);
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
        }
        lottery();
    }

    public boolean holdsLock(TransactionId tid) {
        return holdsSLock(tid) || holdsXLock(tid);
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
             * An edge case where a single transaction requests both sLock and xLock,
             * although it should never happen as the transaction can simply request a
             * xLock.
             */
            if (sPool.size() == 1) {
                for (Ticket st : sPool) {
                    st.cond.signalAll();
                    TransactionId stid = st.tid;
                    // If the transaction also requests for a xLock, grant it.
                    for (Ticket t : xQueue) {
                        if (t.tid == stid) {
                            t.cond.signalAll();
                            break; // A transaction should have only one ticket in the queue.
                        }
                    }
                    xQueue.removeIf(t -> t.tid == stid);
                }
                sPool.clear();
                return;
            }
            /*
             * Note(Qing): We first have all sLocks granted together but only notify one
             * single winner among xLock requests to avoid herd effect.
             * However, can xLock requests starve?
             */
            if (sPool.size() > 1) {
                for (Ticket t : sPool) {
                    t.cond.signalAll();
                }
                sPool.clear();
            } else if (!xQueue.isEmpty()) {
                Ticket winner = xQueue.remove(0);
                winner.cond.signalAll();
            }
        }
    }

}