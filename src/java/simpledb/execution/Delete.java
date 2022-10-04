package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private static final TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"num_deleted"});
    private final TransactionId tid;
    private OpIterator child;

    private boolean done = false;
    private Tuple res; // contains number of affected tuples

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        tid = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        deleteTuples();
        super.open();
    }

    private void deleteTuples() throws DbException, TransactionAbortedException {
        int num = 0;
        Tuple tu;
        while (child.hasNext()) {
            tu = child.next();
            try {
                Database.getBufferPool().deleteTuple(tid, tu);
            } catch (IOException e) {
                e.printStackTrace();
                throw new DbException("fail to delete tuple due to IO error");
            }
            num++;
        }
        res = new Tuple(td);
        res.setField(0, new IntField(num));
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        done = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (done)
            return null;
        done = true;
        return res;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (child != children[0]) {
            child = children[0];
        }
    }

}
