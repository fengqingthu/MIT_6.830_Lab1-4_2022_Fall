package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final TupleDesc td;
    private final File file;
    private RandomAccessFile rFile;

    /*
     * For efficient deletion/insertion of tuples into the file. The freeList
     * maintains a stack of page numbers that has empty slot(s).
     * 
     * Note(Qing): This is implemented in Lab2 but deprecated in Lab4 as we do not
     * want a table-level lock while there can be concurrent updates to freeLists in
     * table-level. However, page-level freeLists remain the same.
     */
    // private FreeList<Integer> freeList;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        this.td = td;
        /* Initialize random access file, never close until HeapFile is freed. */
        try {
            rFile = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1); // file is corrupted.
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /* Helper method to calculate page header size based on the schema. */
    private int getPageHeaderSize() {
        int numTuples = (int) Math.floor((BufferPool.getPageSize() * 8f) / (td.getSize() * 8f + 1f));
        return (int) Math.ceil(numTuples / 8f);
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if (pid.getTableId() != getId())
            throw new IllegalArgumentException("page does not exist in this file");

        try {
            rFile.seek(pid.getPageNumber() * BufferPool.getPageSize());
            byte[] data = new byte[BufferPool.getPageSize()];
            rFile.readFully(data, 0, BufferPool.getPageSize());

            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new RuntimeException("Fail to read page from disk");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        if (page.getId().getTableId() != getId())
            throw new IllegalArgumentException("page does not exist in this file");
        int pgNo = page.getId().getPageNumber();
        rFile.seek(pgNo * BufferPool.getPageSize());
        rFile.write(page.getPageData(), 0, BufferPool.getPageSize());
    }

    /**
     * Returns the number of pages in this HeapFile (pages on disk + newly created
     * dirty pages in memory).
     */
    public int numPages() {
        return (int) Math.ceil(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> res = new ArrayList<Page>();
        int pgNo = findFreePage(tid);
        if (pgNo == -1) {
            /* Need to allocate a new page on disk. */
            int newPgNo = numPages();
            HeapPageId newPid = new HeapPageId(getId(), newPgNo);
            HeapPage newPg = new HeapPage(
                    newPid,
                    HeapPage.createEmptyPageData());
            /*
             * NOTE(Qing): Immediately writing empty new page to disk, and read it back from
             * the buffer pool.
             */
            writePage(newPg);

            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, newPid,
                    Permissions.READ_WRITE);
            pg.insertTuple(t);
            res.add(pg);

        } else {
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo),
                    Permissions.READ_WRITE);
            pg.insertTuple(t);
            res.add(pg);
        }
        return res;
    }

    /**
     * Scan through all pages in this table looking for a page with at least one
     * empty slot.
     * 
     * @return the pageNo if a free page exists, otherwise -1.
     */
    private int findFreePage(TransactionId tid) throws TransactionAbortedException, DbException {
        for (int i = numPages() - 1; i >= 0; i--) {
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i),
                    Permissions.READ_ONLY);
            if (pg.getNumUnusedSlots() > 0) {
                return i;
            }
            /*
             * Note(Qing): Here, can safely release the read lock if no empty slot found in
             * this page. This read-only op does not affect consistency.
             */
            pg.getPgLock().sUnlock(tid);
        }
        return -1;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> res = new ArrayList<Page>();
        if (t.getRecordId().getPageId().getTableId() != getId())
            throw new DbException("tuple not found in this table");

        HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),
                Permissions.READ_WRITE);
        pg.deleteTuple(t);

        res.add(pg);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}
