package simpledb.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import simpledb.transaction.TransactionId;

/**
 * A helper class to detect and handle deadlocks, implementing a quite
 * passive deadlock handling mechansim - the deadlock handler essentially
 * kicks off a background thread that builds the wait-for graph and detects
 * all cycles, only when the waitMap has paused for a wide time threshold.
 * 
 * The rationale is that the holders of locks are frequently changing, so the
 * wait-for graph also varies quickly and it would be really expensive to
 * track the wait-for graph in real-time and detect any incoming cycles. We
 * kinda "batchlize" the cycles within the time threshold and only abort
 * when we really have to do so.
 */
public class DeadLockHandler {
    private static final long INTERVAL = 10;
    private static final long THRESHOLD = 100;
    private long lastUpdate;
    private long lastCheck;
    private final HashMap<TransactionId, Set<PageLock>> waitMap;
    private final HashMap<TransactionId, Thread> threadMap;

    public DeadLockHandler() {
        lastCheck = lastUpdate = System.currentTimeMillis();
        waitMap = new HashMap<>();
        threadMap = new HashMap<>();
        // Kick-off a thread in background that periodically detects and handles
        // deadlocks.
        Runnable detector = new Runnable() {
            public void run() {
                detectDeadLock();
            }
        };
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(detector, INTERVAL, INTERVAL, TimeUnit.MILLISECONDS);
    }

    public synchronized void waitFor(TransactionId tid, PageLock lock) {
        lastUpdate = System.currentTimeMillis();

        threadMap.put(tid, Thread.currentThread());
        if (!waitMap.containsKey(tid)) {
            waitMap.put(tid, new HashSet<>());
        }
        waitMap.get(tid).add(lock);
    }

    /**
     * This dirty API is for lock manager commiting transactions. Ideally we should
     * build a global thread pool to map transactions to threads and refactor the
     * deadlock detector to only talk with individual page locks.
     */
    public synchronized void removeThread(TransactionId tid) {
        lastUpdate = System.currentTimeMillis();
        threadMap.remove(tid);
    }

    /** It acts as a no-op if the transaction is not waiting on the lock. */
    public synchronized void unwait(TransactionId tid, PageLock lock) {
        lastUpdate = System.currentTimeMillis();

        if (waitMap.containsKey(tid))
            waitMap.get(tid).remove(lock);
    }

    /**
     * The whole deadlock handler class is synchronized so we know when this method
     * is running, the state of waitMap is determined. Since grabing locks
     * involves changing the waitMap, we know the wait-for graph to be built is
     * also determined.
     */
    private synchronized void detectDeadLock() {
        /*
         * Building wait-for graph and detect for all cycles can be rather expensive as
         * the nunmber of cycles grow exponentially. There, we only do so after the
         * wait-for graph has paused for a rather large time threshold.
         */
        long now = System.currentTimeMillis();
        if (now - lastUpdate < THRESHOLD || now - lastCheck < THRESHOLD) {
            return;
        }

        Set<TransactionId> toAbort = new HashSet<>();
        Set<TransactionId> seen = new HashSet<>();

        /* Brute-force all-simple-cycle-detection: simply run DFS on all nodes. */
        for (TransactionId root : waitMap.keySet()) {
            if (!seen.contains(root)) {
                seen.add(root);
                List<TransactionId> path = new ArrayList<>(Arrays.asList(root));
                Set<Set<TransactionId>> cycles = new HashSet<>();
                dfs(root, seen, path, cycles);

                // WAIT-WOUND: If cycles detected, abort the youngest transactions. The oldest
                // transaction will never be aborted and thus keep the DB progressing.
                if (!cycles.isEmpty()) {

                    for (Set<TransactionId> cycle : cycles) {
                        TransactionId t = null;
                        long mx = Long.MIN_VALUE;
                        for (TransactionId node : cycle) {
                            if (node.getId() > mx) {
                                mx = node.getId();
                                t = node;
                            }
                        }
                        toAbort.add(t);
                    }
                }

                /* If all threads except the oldest one have to abort, can break early. */
                if (toAbort.size() == threadMap.size() - 1) {
                    break;
                }
            }
        }

        if (toAbort.size() > 0) {
            System.out.println(String.format("Abort %d transactions", toAbort.size()));
        }
        toAbort.forEach(tid -> abort(tid));
        // Update time stamp here to avoid duplicated runs of cycle-detection.
        lastCheck = System.currentTimeMillis();
    }

    /**
     * Recursive Depth-First-Search to find all simple-cycles starting with the
     * root (start node).
     * 
     * @param node   The current node to visit.
     * @param seen   The set of processed start nodes.
     * @param path   The current visiting path. The first element is the start.
     * @param cycles Found cycles that start with the root.
     */
    private void dfs(TransactionId node, Set<TransactionId> seen, List<TransactionId> path,
            Set<Set<TransactionId>> cycles) {
        if (waitMap.containsKey(node)) {
            for (PageLock lock : waitMap.get(node)) {
                for (TransactionId child : lock.getHolders()) {
                    if (child == node)
                        continue; // Ignore self-loop.
                    if (child == path.get(0) && path.size() > 1) {
                        cycles.add(new HashSet<>(path));
                        continue;
                    }
                    if (!path.contains(child) && !seen.contains(child)) {
                        path.add(child);
                        dfs(child, seen, path, cycles);
                        path.remove(child);
                    }
                }
            }
        }
    }

    /**
     * Abort a given transaction by sending an interruption to it. We only do so
     * when the thread is waiting on certain lock, checked by a rather dirty hack.
     */
    private void abort(TransactionId tid) {
        /*
         * Note(Qing): A hacky protection against interrupting some random threads
         * that are not wating for locks, which may cause errors as InterruptedException
         * would be caught somewhere else. Also, each transaction should only be aborted
         * at most once.
         */
        if (!threadMap.containsKey(tid))
            return;
        StackTraceElement[] s = threadMap.get(tid).getStackTrace();
        boolean found = false;
        for (StackTraceElement elt : s) {
            if (elt.getMethodName().contains("xLock") || elt.getMethodName().contains("sLock")) {
                found = true;
                break;
            }
        }
        if (!found)
            return;

        threadMap.get(tid).interrupt();
        threadMap.remove(tid);
    }

}