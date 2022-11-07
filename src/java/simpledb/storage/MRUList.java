package simpledb.storage;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * A helper data structure used to build MRU. This java linkedList does not
 * provide O(1) unlink API but should be fast enough when given the index as
 * there is not memory copy but just pointer moves.
 */
public class MRUList<E> {

    private final int capacity;
    private LinkedList<E> ll;
    private HashMap<E, Integer> h; // map elements to indices in ll.

    public MRUList(int capacity) throws IllegalArgumentException {
        if (capacity <= 0)
            throw new IllegalArgumentException("capacity must be a positive int");
        this.capacity = capacity;
        ll = new LinkedList<E>();
        h = new HashMap<E, Integer>();
    }

    public synchronized int size() {
        return h.size();
    }

    private synchronized void append(E e) {
        h.put(e, ll.size());
        ll.addLast(e);
    }

    /**
     * Add one element to MRU cache, evict the most recently used one if the cache
     * is full.
     * 
     * @param e the element to add.
     * @return the evicted element or null if no eviction happens.
     */
    public synchronized E add(E e) {
        if (h.containsKey(e)) {
            ll.remove(h.get(e));
            append(e);
            return null;
        }
        if (size() < capacity) {
            append(e);
            return null;
        }
        E res = ll.removeLast();
        h.remove(res);
        append(e);
        return res;
    }

    /**
     * Evict the most recently used element.
     * 
     * @return the evicted element or null if mru is empty.
     */
    public synchronized E evict() {
        if (size() == 0)
            return null;
        E res = ll.removeLast();
        h.remove(res);
        return res;
    }

    /**
     * Remove the given element, acts as a no-op if not found.
     * 
     * @param e the element to remove.
     */
    public synchronized void remove(E e) {
        if (!h.containsKey(e))
            return;
        ll.remove(h.get(e));
        h.remove(e);
    }

}