package simpledb.execution;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A helper class to calculate subsets of a set with specified size.
 */
public class SubsetIterator<T> implements Iterator<Set<T>> {
    private List<T> vals = null;
    private final int size;
    private Iterator<T> iter0;
    private SubsetIterator<T> subiter1 = null;
    private SubsetIterator<T> subiter2 = null;
    private boolean done = false;

    public SubsetIterator(List<T> vals, int size) {
        if (size < 0 || size > vals.size()) {
            throw new IllegalArgumentException("Illegal subset size");
        }
        this.size = size;
        this.vals = vals;
        if (size == 0 || size == vals.size())
            return;
        if (size == 1) {
            iter0 = vals.iterator();
            return;    
        }
        subiter1 = new SubsetIterator<T>(vals.subList(1, vals.size()), size);
        subiter2 = new SubsetIterator<T>(vals.subList(1, vals.size()), size - 1);
    }

    public boolean hasNext() {
        if (size == 0 || size == vals.size())
            return !done;
        if (size == 1)
            return iter0.hasNext();
        return subiter1.hasNext() || subiter2.hasNext();
    }

    public Set<T> next() {
        if (!hasNext())
            throw new NoSuchElementException("Calling next on finished iterator.");
        if (size == 0) {
            done = true;
            return new HashSet<T>();
        }
        if (size == vals.size()) {
            done = true;
            return new HashSet<T>(vals);
        }
        if (size == 1) {
            return new HashSet<T>(Arrays.asList(iter0.next()));
        }
        if (subiter1.hasNext()) {
            return subiter1.next();
        } 
        Set<T> res = subiter2.next();
        res.add(vals.get(0));
        return res;
    }
}
