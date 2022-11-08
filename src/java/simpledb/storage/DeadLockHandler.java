package simpledb.storage;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
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
    private static final long THRESHOLD = 100;
    private long lastUpdate;
    private long lastCheck;
    private final Random rand;
    private final HashMap<TransactionId, Set<PageLock>> waitMap;
    private final HashMap<TransactionId, Thread> threadMap;

    public DeadLockHandler() {
        lastCheck = lastUpdate = System.currentTimeMillis();
        rand = new Random();
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
         * Building wait-for graph and detect for cycles can be rather expensive. Only
         * do so after the wait-for graph pauses for a time threshold.
         */
        long now = System.currentTimeMillis();
        if (now - lastUpdate < THRESHOLD || now - lastCheck < THRESHOLD) {
            return;
        }
        lastCheck = now;
        // System.out.println(
        //         String.format("[%d], A deadlock may have occured, building wait-for graph and doing DFS", now));

        // Full-BFS for cycle-detection
        Set<TransactionId> seen = new HashSet<>();
        for (TransactionId root : waitMap.keySet()) {
            if (!seen.contains(root)) {

                Set<TransactionId> currLevel = new HashSet<>(Arrays.asList(root));
                while (!currLevel.isEmpty()) {
                    Set<TransactionId> nextLevel = new HashSet<>();
                    for (TransactionId node : currLevel) {
                        if (waitMap.containsKey(node)) {
                            for (PageLock lock : waitMap.get(node)) {
                                nextLevel.addAll(lock.getHolders());
                            }
                        }
                    }

                    if (!Collections.disjoint(seen, nextLevel)) {
                        System.out.println("Cycle detected");

                        HashSet<TransactionId> ends = new HashSet<>(seen);
                        ends.retainAll(nextLevel);
                        /*
                         * At lease one cycle is detected. Randomly determined to abort the end points
                         * (most likely the root), or the nodes the end points wait on.
                         */
                        if (rand.nextDouble() < 2f) {
                            System.out.println("Aborting ends");
                            for (TransactionId t : ends) {
                                abort(t);
                            }
                        } else {
                            System.out.println("Aborting ends' next hop");
                            for (TransactionId node : ends) {
                                for (PageLock lock : waitMap.get(node)) {
                                    for (TransactionId t : lock.getHolders()) {
                                        abort(t);
                                    }
                                }
                            }
                        }
                        return;
                    } else {
                        seen.addAll(nextLevel);
                        currLevel = nextLevel;
                    }
                }
            }
        }
    }

    private void abort(TransactionId tid) {
        /*
         * Note(Qing): A hack protection against interrupting some random threads
         * that are not wating for locks.
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