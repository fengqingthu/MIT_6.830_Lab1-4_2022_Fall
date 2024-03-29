package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private final int afield;
    private final int gfield;
    private final Op op;
    private OpIterator child;
    private Aggregator aggr;
    private OpIterator aggrIter;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op op) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.op = op;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        populateAggregator();
        super.open();
    }

    /*
     * Load all tuples from child iterator into corresponding aggregator. Must be
     * called when child has been opened.
     */
    private void populateAggregator() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        Type gfieldType = null;
        if (gfield != Aggregator.NO_GROUPING)
            gfieldType = child.getTupleDesc().getFieldType(gfield);
        
        if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE)
            aggr = new IntegerAggregator(
                    gfield,
                    gfieldType,
                    afield,
                    op);
        else
            aggr = new StringAggregator(
                    gfield,
                    gfieldType,
                    afield,
                    op);

        while (child.hasNext()) {
            aggr.mergeTupleIntoGroup(child.next());
        }
        aggrIter = aggr.iterator();
        aggrIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggrIter.hasNext())
            return aggrIter.next();
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        aggrIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc childTd = child.getTupleDesc();
        if (gfield == Aggregator.NO_GROUPING)
            return new TupleDesc(
                    new Type[] { Type.INT_TYPE },
                    new String[] {
                            childTd.getFieldName(afield)
                    });
        else
            return new TupleDesc(
                    new Type[] {
                            childTd.getFieldType(gfield),
                            Type.INT_TYPE },
                    new String[] {
                            childTd.getFieldName(gfield),
                            childTd.getFieldName(afield),
                    });
    }

    @Override
    public void close() {
        super.close();
        aggr = null;
        aggrIter = null;
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (child != children[0]) {
            child = children[0];
        }
    }

}
