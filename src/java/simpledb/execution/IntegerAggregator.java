package simpledb.execution;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.storage.Field;
import simpledb.storage.IntField;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gfield;
    private final Type gfieldType;
    private final int afield;
    private final Op op;

    /*
     * Try to minimize the storage overhead by making values integers instead of
     * a list. When no grouping is given, use mere integers no hashmap.
     */
    private HashMap<Field, Integer> gres;
    private int res;
    /* For AVG, main the number of elements. */
    private HashMap<Field, Integer> gnum;
    private int num = 0;

    /**
     * Aggregate constructor
     *
     * @param gfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gfield, Type gfieldtype, int afield, Op what) {
        this.afield = afield;
        op = what;
        if (gfield == NO_GROUPING) {
            this.gfield = NO_GROUPING;
            gfieldType = null;
            /* Initialize res. */
            if (what == Op.MAX)
                res = Integer.MIN_VALUE;
            else if (what == Op.MIN)
                res = Integer.MAX_VALUE;
            else
                res = 0;
        } else {
            this.gfield = gfield;
            gfieldType = gfieldtype;
            gres = new HashMap<Field, Integer>();
            if (what == Op.AVG)
                gnum = new HashMap<Field, Integer>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tu the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tu) {
        if (gfield == NO_GROUPING) {
            mergeTupleWithoutGroup(tu);
        } else {
            /* Initialize res. */
            int res;
            Field key = tu.getField(gfield);
            if (op == Op.MAX)
                res = gres.getOrDefault(key, Integer.MIN_VALUE);
            else if (op == Op.MIN)
                res = gres.getOrDefault(key, Integer.MAX_VALUE);
            else
                res = gres.getOrDefault(key, 0);

            /* Force downcast as this is an interger aggregator. */
            int val = ((IntField) tu.getField(afield)).getValue();
            switch (op) {
                case COUNT:
                    gres.put(key, ++res);
                    break;
                case SUM:
                    gres.put(key, res + val);
                    break;
                case MAX:
                    gres.put(key, Math.max(res, val));
                    break;
                case MIN:
                    gres.put(key, Math.min(res, val));
                    break;
                case AVG:
                    gres.put(key, res + val);
                    gnum.put(key, gnum.getOrDefault(key, 0) + 1);
                    break;
                default:
                    throw new RuntimeException("not implemented");
            }
        }
    }

    /*
     * Separate a private method to merge tuple when no grouping required. This is
     * to avoid the overhead of maintaining a hashmap.
     */
    private void mergeTupleWithoutGroup(Tuple tu) {
        int val = ((IntField) tu.getField(afield)).getValue();
        switch (op) {
            case COUNT:
                res++;
                break;
            case SUM:
                res += val;
                break;
            case MAX:
                res = Math.max(res, val);
                break;
            case MIN:
                res = Math.min(res, val);
                break;
            case AVG:
                /* For accuracy, not using a running avg. res here means sum. */
                res += val;
                num++;
                break;
            default:
                throw new RuntimeException("not implemented");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        if (gfield == NO_GROUPING)
            return iteratorWithoutGroup();
        TupleDesc td = new TupleDesc(new Type[] { gfieldType, Type.INT_TYPE });

        Tuple res[] = new Tuple[gres.size()];
        int i = 0;
        for (Map.Entry<Field, Integer> entry : gres.entrySet()) {
            Field key = entry.getKey();
            int val = entry.getValue();
            res[i] = new Tuple(td);
            res[i].setField(0, key);
            if (op == Op.AVG)
                res[i].setField(1, new IntField(val / gnum.get(key)));
            else
                res[i].setField(1, new IntField(val));
            i++;
        }

        return new TupleIterator(td, Arrays.asList(res));
    }

    /*
     * Also separate a private method to return iterator when no grouping is given.
     */
    private OpIterator iteratorWithoutGroup() {
        TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });

        Tuple tu = new Tuple(td);
        if (op == Op.AVG)
            tu.setField(0, new IntField(res / num));
        else
            tu.setField(0, new IntField(res));

        return new TupleIterator(td, Arrays.asList(new Tuple[] { tu }));
    }

}
