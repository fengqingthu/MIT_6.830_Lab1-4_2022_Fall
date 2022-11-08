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
    private static final long THRESHOLD = 500;
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

    /** It acts as a no-op if the transaction is not waiting on the lock. */
    public synchronized void unwait(TransactionId tid, PageLock lock) {
        lastUpdate = System.currentTimeMillis();

        threadMap.remove(tid);
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
         * Building wait-for graph and detect for all cycles can be rather expensive.
         * Only do so after the wait-for graph has paused for a rather large time
         * threshold.
         */
        long now = System.currentTimeMillis();
        if (now - lastUpdate < THRESHOLD || now - lastCheck < THRESHOLD) {
            return;
        }
        lastCheck = now;

        Set<Set<TransactionId>> cycles = new HashSet<>();

        /* Brute-force for all-simple-cycle-detection: simply run DFS on all nodes. */
        for (TransactionId root : waitMap.keySet()) {
            List<TransactionId> path = new ArrayList<>(Arrays.asList(root));
            dfs(root, path, cycles);
        }

        // WAIT-WOUND: If cycles detected, abort the youngest transactions.
        if (!cycles.isEmpty()) {
            Set<TransactionId> toAbort = new HashSet<>();

            // System.out.println(String.format("%d cycles detected.", cycles.size()));
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

            toAbort.forEach(tid -> abort(tid));
        }
    }

    /**
     * Recursive Depth-First-Search.
     * 
     * @param node   The current node to visit.
     * @param path   The current visiting path. The first element is the start.
     * @param cycles Found cycles are added into this set.
     */
    private void dfs(TransactionId node, List<TransactionId> path, Set<Set<TransactionId>> cycles) {
        if (waitMap.containsKey(node)) {
            for (PageLock lock : waitMap.get(node)) {
                for (TransactionId child : lock.getHolders()) {
                    if (child == path.get(0) && path.size() > 1) {
                        cycles.add(new HashSet<>(path));
                        continue;
                    }
                    if (!path.contains(child)) {
                        path.add(child);
                        dfs(child, path, cycles);
                        path.remove(child);
                    }
                }
            }
        }
    }

    /**
     * Abort a given transaction by sending an interruption to it. We only do so
     * when the thread is confirmed waiting on certain lock, by a nasty hack.
     */
    private void abort(TransactionId tid) {
        /*
         * Note(Qing): A hack protection against interrupting some random threads
         * that are not wating for locks, which may cause errors as InterruptedException
         * would be caught somewhere else.
         */
        StackTraceElement[] s = threadMap.get(tid).getStackTrace();
        boolean found = false;
        for (StackTraceElement elt : s) {
            if (elt.getMethodName().contains("xLock") || elt.getMethodName().contains("sLock"))
                found = true;
        }
        if (!found)
            return;

        threadMap.get(tid).interrupt();
    }

}