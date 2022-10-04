package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private static final TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"num_inserted"});
    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;

    private boolean done = false;
    private Tuple res; // contains number of affected tuples

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        if (!child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(tableId).getTupleDesc()))
            throw new DbException("trying to insert tuples withou mismatching schema into a table");
        tid = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    private void insertTuples() throws DbException, TransactionAbortedException {
        int num = 0;
        Tuple tu;
        while (child.hasNext()) {
            tu = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, tu);
            } catch (IOException e) {
                e.printStackTrace();
                throw new DbException("fail to insert tuple due to IO error");
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (done)
            return null;
        insertTuples();
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
