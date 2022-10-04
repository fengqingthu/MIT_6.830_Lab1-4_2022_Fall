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
import java.util.Random;

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
     */
    private FreeList<Integer> freeList;

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
        freeList = new FreeList<Integer>();
        populateFreeList();
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

    /*
     * Scan through the page headers of the given file and build the freeList and
     * freeMap data structures. Should be called by the constructor.
     */
    private void populateFreeList() {
        int pgHeaderSize = getPageHeaderSize();
        for (int pgNo = 0; pgNo < numPages(); pgNo++) {
            try {
                rFile.seek(pgNo * BufferPool.getPageSize());
                byte[] header = new byte[pgHeaderSize];
                rFile.readFully(header, 0, pgHeaderSize);

                /* If contains at least one empty slot, add to freeList. */
                for (int i = 0; i < pgHeaderSize; i++) {
                    if (header[i] != (byte) ~0) {
                        freeList.append(pgNo);
                        break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1); // file is corrupted
            }
        }
    }

    /* Helper method to calculate page header size based on the schema. */
    private int getPageHeaderSize() {
        int numTuples = (int) Math.floor((BufferPool.getPageSize() * 8f) / (td.getSize() * 8f + 1f));
        return (int) Math.ceil(numTuples / 8f);
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        if (pid.getTableId() != getId())
            throw new IllegalArgumentException("page does not exist in this file");

        try {
            rFile.seek(pid.getPageNumber() * BufferPool.getPageSize());
            byte[] data = new byte[BufferPool.getPageSize()];
            rFile.readFully(data, 0, BufferPool.getPageSize());

            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        if (page.getId().getTableId() != getId())
            throw new IllegalArgumentException("page does not exist in this file");
        int pgNo = page.getId().getPageNumber();
        rFile.seek(pgNo * BufferPool.getPageSize());
        rFile.write(page.getPageData(), 0, BufferPool.getPageSize());

        /* Need to update the freeList. */
        HeapPage hpg = (HeapPage) page;
        if (hpg.getNumUnusedSlots() > 0 && !freeList.contains(pgNo)) {
            freeList.append(pgNo);
        } else if (hpg.getNumUnusedSlots() == 0 && freeList.contains(pgNo)) {
            freeList.remove(pgNo);
        }
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

        if (freeList.size() == 0) {
            /* Need to allocate a new page on disk. */
            int newPgNo = numPages();
            HeapPage newPg = new HeapPage(
                    new HeapPageId(getId(), newPgNo),
                    HeapPage.createEmptyPageData());
            newPg.insertTuple(t);

            if (newPg.getNumUnusedSlots() > 0) {
                freeList.append(newPgNo);
            }
            /*
             * NOTE(Qing): Immediately writing new page back to disk, but also mark
             * this page dirty (adding to bufferpool) so if this transaction aborts,
             * bufferpool should be responsible to clean it up.
             */
            writePage(newPg);
            res.add(newPg);

        } else {
            int pgNo = freeList.peek();
            HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo),
                    Permissions.READ_WRITE);
            pg.insertTuple(t);

            /* If this page newly becomes full, remove it from freeList. */
            if (pg.getNumUnusedSlots() == 0) {
                freeList.remove(pgNo);
            }
            res.add(pg);
        }
        return res;
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
        /* If this page newly becomes available, add it to freeList. */
        if (pg.getNumUnusedSlots() == 1) {
            freeList.append(t.getRecordId().getPageId().getPageNumber());
        }
        res.add(pg);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}
