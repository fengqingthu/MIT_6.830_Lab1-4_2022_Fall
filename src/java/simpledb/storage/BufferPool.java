package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private final int maxNumPages;
    private final ConcurrentHashMap<PageId, Page> pidToPage;
    private final ConcurrentHashMap<TransactionId, Set<PageLock>> lockMap;

    private final MRUList<PageId> mru;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        maxNumPages = numPages;
        pidToPage = new ConcurrentHashMap<PageId, Page>();
        mru = new MRUList<PageId>(numPages);
        lockMap = new ConcurrentHashMap<>();
    }

    public ConcurrentHashMap<TransactionId, Set<PageLock>> getLockMap() {
        return lockMap;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        Page page = pidToPage.get(pid);
        if (page != null) {
            grabLock(tid, page, perm);

            return page;
        }
        if (pidToPage.size() == maxNumPages)
            evictPage();
        /* Read the page from disk and add to buffer. */
        try {
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pidToPage.put(pid, page);
            mru.add(pid);
            grabLock(tid, page, perm);

            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DbException("Fail to load page from disk\n");
        }
    }

    private void grabLock(TransactionId tid, Page page, Permissions perm) throws TransactionAbortedException {
        if (!lockMap.containsKey(tid)) {
            lockMap.put(tid, new HashSet<PageLock>());
        }
        lockMap.get(tid).add(page.getPgLock());
        if (perm == Permissions.READ_ONLY)
            page.getPgLock().sLock(tid);
        if (perm == Permissions.READ_WRITE)
            page.getPgLock().xLock(tid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        PageLock lock = pidToPage.get(pid).getPgLock();
        lock.releaseAll(tid);
        lockMap.get(tid).remove(lock);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        if (!pidToPage.containsKey(pid))
            return false;
        Page pg = pidToPage.get(pid);
        return pg.getPgLock().holdsLock(tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (!commit) {
            for (Entry<PageId, Page> entry : pidToPage.entrySet()) {
                if (entry.getValue().isDirty() == tid) {
                    removePage(entry.getKey());
                }
            }
        } else {
            try {
                flushPages(tid);
            } catch (IOException e) {
                System.exit(1); // FORCE is broken, exit.
            }
        }

        if (lockMap.containsKey(tid)) {
            for (PageLock lock: lockMap.get(tid)) {
                lock.releaseAll(tid);
            }
            lockMap.remove(tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> dirtyPages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page pg : dirtyPages) {
            pg.markDirty(true, tid);
            pidToPage.put(pg.getId(), pg);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> dirtyPages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId())
                .deleteTuple(tid, t);
        for (Page pg : dirtyPages) {
            pg.markDirty(true, tid);
            pidToPage.put(pg.getId(), pg);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pidToPage.keySet()) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        mru.remove(pid);
        pidToPage.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pidToPage.get(pid));
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Entry<PageId, Page> entry : pidToPage.entrySet()) {
            if (entry.getValue().isDirty() == tid) {
                flushPage(entry.getKey());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        ArrayList<PageId> dirtyPages = new ArrayList<>();
        PageId pid = mru.evict();

        while (pidToPage.get(pid).isDirty() != null) {
            dirtyPages.add(pid);
            pid = mru.evict();
            if (pid == null) {
                Collections.reverse(dirtyPages);
                for (PageId p : dirtyPages) {
                    mru.add(p);
                }
                throw new DbException("Fail to evict. All pages in the buffer pool are dirty.");
            }
        }

        Collections.reverse(dirtyPages);
        for (PageId p : dirtyPages) {
            mru.add(p);
        }

        try {
            flushPage(pid);
        } catch (Exception e) {
            throw new DbException(String.format("Fail to flush evicted page to disk, pid: %s", pid.toString()));
        }
        pidToPage.remove(pid);
    }

}
