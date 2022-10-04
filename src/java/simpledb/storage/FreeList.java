package simpledb.storage;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * A helper data structure that works as a hashset that maintains
 * a sequence order and supports append and pop.
 */
public class FreeList<E> {

    /*
     * Actually freeList should prefer linkedlist implementation but java linkedlist
     * does not provide public unlink API. ArrayList is also fine.
     */
    private ArrayList<E> list;
    private HashMap<E, Integer> h;

    public FreeList() {
        list = new ArrayList<E>();
        h = new HashMap<E, Integer>();
    }

    public int size() {
        return h.size();
    }

    /**
     * No-op if already contains e.
     * 
     * @param e element to append to the list.
     */
    public void append(E e) {
        if (contains(e))
            return;
        h.put(e, list.size());
        list.add(e);
    }

    public E pop() {
        E res = list.get(list.size() - 1);
        h.remove(res);
        list.remove(list.size() - 1);
        return res;
    }

    /**
     * O(1) deletion. No-op if e is not in freeList.
     * 
     * @param e element to be removed.
     */
    public void remove(E e) {
        Integer idx = h.get(e);
        if (idx == null)
            // throw new IllegalArgumentException("element not found");
            return;
        int end = list.size() - 1;
        if (idx == end) {
            h.remove(e);
            list.remove(idx);
            return;
        }
        list.set(idx, list.get(end));
        h.put(list.get(idx), idx);
        h.remove(e);
        list.remove(end);
    }

    /** Peek the end of the sequence. Should be called when not empty. */
    public E peek() {
        return list.get(list.size() - 1);
    }

    public boolean contains(E e) {
        return h.containsKey(e);
    }

    public String toString() {
        if (size() == 0)
            return "[]";
        String res = "[";
        int end = size() - 1;
        for (int i = 0; i < end; i++)
            res += list.get(i).toString() + ", ";
        return res + list.get(end).toString() + "]";
    }

}