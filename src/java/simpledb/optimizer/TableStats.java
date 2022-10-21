package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;
    private final int ioCostPerPage;
    private final int tableid;
    private int ntups = 0;
    // Use two hashtables to store histograms.
    private HashMap<Integer, StringHistogram> strHists;
    private HashMap<Integer, IntHistogram> intHists;
    // Number of distinct values, for join cardinality estimate.
    private HashMap<Integer, Integer> numDistinct;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate
     *                      between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        TupleDesc schema = Database.getCatalog().getTupleDesc(tableid);
        int numFields = schema.numFields();
        strHists = new HashMap<Integer, StringHistogram>();
        intHists = new HashMap<Integer, IntHistogram>();
        numDistinct = new HashMap<Integer, Integer>();
        HashMap<Integer, HashSet<String>> seenStr = new HashMap<Integer, HashSet<String>>();
        HashMap<Integer, HashSet<Integer>> seenInt = new HashMap<Integer, HashSet<Integer>>();

        HashMap<Integer, Integer> mins = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> maxs = new HashMap<Integer, Integer>();

        for (int i = 0; i < numFields; i++) {
            if (schema.getFieldType(i) == Type.INT_TYPE) {
                mins.put(i, Integer.MAX_VALUE);
                maxs.put(i, Integer.MIN_VALUE);
                seenInt.put(i, new HashSet<Integer>());
            } else {
                strHists.put(i, new StringHistogram(NUM_HIST_BINS));
                seenStr.put(i, new HashSet<String>());
            }
        }

        SeqScan scan = new SeqScan(new TransactionId(), tableid);
        try {
            scan.open();
            // First scan, sample min and max of int columns, instantiate IntHistograms.
            while (scan.hasNext()) {
                Tuple tu = scan.next();
                ntups++;
                for (int i = 0; i < numFields; i++) {
                    if (mins.containsKey(i)) {
                        int val = ((IntField) tu.getField(i)).getValue();
                        mins.put(i, Math.min(mins.get(i), val));
                        maxs.put(i, Math.max(maxs.get(i), val));
                    }
                }
            }
            for (int i : mins.keySet()) {
                intHists.put(i, new IntHistogram(NUM_HIST_BINS, mins.get(i), maxs.get(i)));
            }

            scan.rewind();
            // Second scan, load cell values into hists. And calculate distinct values.
            while (scan.hasNext()) {
                Tuple tu = scan.next();
                for (int i = 0; i < numFields; i++) {
                    if (strHists.containsKey(i)) {
                        String val = ((StringField) tu.getField(i)).getValue();
                        strHists.get(i).addValue(val);
                        seenStr.get(i).add(val);
                    } else {
                        int val = ((IntField) tu.getField(i)).getValue();
                        intHists.get(i).addValue(val);
                        seenInt.get(i).add(val);
                    }
                }
            }
            scan.close();
            // Update numDistinct.
            for (int i : seenInt.keySet()) {
                numDistinct.put(i, seenInt.get(i).size());
            }
            for (int i : seenStr.keySet()) {
                numDistinct.put(i, seenStr.get(i).size());
            }
        } catch (Exception e) {
            System.out.printf("Fail to construct stats of tableid= %d\n", tableid);
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * @param field The index of the field in a row.
     * @return The number of distinct values in the given column.
     */
    public int getNumDistinct(int field) {
        Integer res = numDistinct.get(field);
        if (res != null)
            return res;
        throw new IllegalArgumentException("Input filed index out of range.");
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // For now we force downcast to HeapFile.
        return ((HeapFile) Database.getCatalog().getDatabaseFile(tableid)).numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) Math.round(ntups * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then
     *              given a tuple, of which we do not know the value of the field,
     *              return the expected selectivity. You may estimate this value
     *              from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        if (strHists.containsKey(field)) {
            return strHists.get(field).avgSelectivity();
        } else if (intHists.containsKey(field)) {
            return intHists.get(field).avgSelectivity();
        } else
            throw new IllegalArgumentException("Input field index out of range.");
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (strHists.containsKey(field)) {
            return strHists.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        } else if (intHists.containsKey(field)) {
            return intHists.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        } else
            throw new IllegalArgumentException("Input field index out of range.");
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return ntups;
    }

}
