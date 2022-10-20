package simpledb.optimizer;

import java.util.HashMap;

import simpledb.execution.Predicate;
import simpledb.execution.Predicate.Op;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {

    private HashMap<Integer, Integer> hist;
    private int[] b_width;
    private final int buckets;
    private final double step;
    private final int min;
    private final int max;
    private int ntups = 0;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it
     * receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through
     * the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed. For
     * example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this
     *                class for histogramming
     * @param max     The maximum integer value that will ever be passed to this
     *                class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        hist = new HashMap<Integer, Integer>();
        b_width = new int[buckets];
        // If num of buckets larger than integers in the range, only use the front of buckets.
        if (buckets > (max - min + 1))
            buckets = (max - min + 1);

        this.buckets = buckets;
        this.min = min;
        this.max = max;
        step = (max - min + 1) / (double) buckets;
        // Initialize histogram
        for (int i = 0; i < buckets; i++) {
            hist.put(i, 0);
            b_width[i] = (int) Math.floor((i + 1) * step) - (int) Math.floor(i * step);
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int b = computeBucket(v);
        hist.put(b, hist.get(b) + 1);
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int b = computeBucket(v);
        double ans;
        switch (op) {
            case EQUALS:
                if (v > max || v < min)
                    return 0f;
                return ((double) hist.get(b) / b_width[b]) / ntups;
            case GREATER_THAN:
                if (v >= max)
                    return 0f;
                if (v < min)
                    return 1f;
                int b_right = Math.max((int) Math.floor(min + (b + 1) * step) - 1, (int) Math.floor(min + b * step));
                ans = (hist.get(b) / (double) ntups) * ((b_right - v + 1) / b_width[b]);
                for (int i = b + 1; i < buckets; i++) {
                    ans += hist.get(i) / (double) ntups;
                }
                return ans;
            case LESS_THAN:
                if (v > max)
                    return 1f;
                if (v <= min)
                    return 0f;
                int b_left = (int) Math.floor(min + b * step);
                ans = (hist.get(b) / (double) ntups) * ((v - b_left + 1) / b_width[b]);
                for (int i = 0; i < b; i++) {
                    ans += hist.get(i) / (double) ntups;
                }
                return ans;
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Op.EQUALS, v) + estimateSelectivity(Op.GREATER_THAN, v);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Op.EQUALS, v) + estimateSelectivity(Op.LESS_THAN, v);
            case NOT_EQUALS:
                return 1f - estimateSelectivity(Op.EQUALS, v);
            default:
                throw new IllegalArgumentException("Illegal op for int histogram estimation\n");
        }
    }

    private int computeBucket(int v) {
        return (int) Math.floor((v - min) / step);
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String ans = "";
        for (int i = 0; i < buckets; i++) {
            ans += " [" + String.valueOf((int) Math.floor(min + i * step)) + ", "
                    + String.valueOf((int) Math.floor(min + (i + 1) * step)) + "): " + String.valueOf(hist.get(i));
        }
        return ans;
    }
}
