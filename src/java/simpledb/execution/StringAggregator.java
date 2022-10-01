package simpledb.execution;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gfield;
    private final Type gfieldType;
    private HashMap<Field, Integer> gct;
    private int ct = 0;

    /**
     * Aggregate constructor
     *
     * @param gfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gfield, Type gfieldtype, int afield, Op what) {
        if (what != Op.COUNT)
            throw new IllegalArgumentException("strings can only be aggregated by COUNT");
        this.gfield = gfield;
        gfieldType = gfieldtype;
        if (gfield != NO_GROUPING)
            gct = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tu the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tu) {
        if (gfield == NO_GROUPING)
            ct++;
        else {
            Field key = tu.getField(gfield);
            gct.put(key, gct.getOrDefault(key, 0) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        if (gfield == NO_GROUPING)
            return iteratorWithoutGroup();
        TupleDesc td = new TupleDesc(new Type[] { gfieldType, Type.INT_TYPE });

        Tuple res[] = new Tuple[gct.size()];
        int i = 0;
        for (Map.Entry<Field, Integer> entry : gct.entrySet()) {
            Field key = entry.getKey();
            int val = entry.getValue();
            res[i] = new Tuple(td);
            res[i].setField(0, key);
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
        tu.setField(0, new IntField(ct));

        return new TupleIterator(td, Arrays.asList(new Tuple[] { tu }));
    }

}
