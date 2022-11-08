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
 * A helper class to detect and handle deadlocks.
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

    public synchronized void unwaitAll(TransactionId tid) {
        lastUpdate = System.currentTimeMillis();

        if (waitMap.containsKey(tid)) {
            waitMap.remove(tid);
        }
        threadMap.remove(tid);
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

        if (waitMap.containsKey(tid))
            waitMap.get(tid).remove(lock);
    }

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

    /* Brute-force for all-simple-cycle-detection: simply run DFS on all nodes. */
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

        // Send the corresponding thread an interruption to abort it.
        threadMap.get(tid).interrupt();
    }

}