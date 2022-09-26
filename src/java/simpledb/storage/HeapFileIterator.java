package simpledb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/* An iterator implementing DbFileIterator that wraps upon a heap file to output its tuples. */
public class HeapFileIterator implements DbFileIterator {
    private final HeapFile hf;
    private final TransactionId tid;
    private int nextPageIdx = 0;
    private HeapPage currentPage = null;
    private Iterator<Tuple> currentIter = null;

    /**
     * Constructs an iterator from the specified heap file.
     *
     * @param tuples The heap file to iterate over
     */
    public HeapFileIterator(TransactionId tid, HeapFile hf) {
        this.hf = hf;
        this.tid = tid;
    }

    public void open() throws TransactionAbortedException, DbException {
        this.currentPage = getPage(this.nextPageIdx++);
        this.currentIter = this.currentPage.iterator();
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (this.currentIter == null)
            return false;
        if (this.currentIter.hasNext())
            return true;
        while (this.nextPageIdx < this.hf.numPages()) {
            this.currentPage = getPage(this.nextPageIdx++);
            this.currentIter = this.currentPage.iterator();
            if (this.currentIter.hasNext())
                return true;
        }
        return false;
    }

    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (this.currentIter == null)
            throw new NoSuchElementException("There are no more tuples\n");
        return this.currentIter.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    public void close() {
        this.nextPageIdx = 0;
        this.currentIter = null;
    }

    private HeapPage getPage(int pageIdx) throws TransactionAbortedException, DbException {
        final HeapPageId pid = new HeapPageId(this.hf.getId(), pageIdx);
        return (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    }
}