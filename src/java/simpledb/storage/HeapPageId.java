package simpledb.storage;

import java.util.Objects;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {

    private final int tableid;
    private final int pageno;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        this.tableid = tableId;
        this.pageno = pgNo;
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        return this.tableid;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *         this PageId
     */
    public int getPageNumber() {
        return this.pageno;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *         the table number and the page number (needed if a PageId is used as a
     *         key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        return Objects.hash(this.tableid, this.pageno);
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *         ids are the same)
     */
    public boolean equals(Object o) {
        try {
            PageId opposite = (PageId) o;
            return this.pageno == opposite.getPageNumber() && this.tableid == opposite.getTableId();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
