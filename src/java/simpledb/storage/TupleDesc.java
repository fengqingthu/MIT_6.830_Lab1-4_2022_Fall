package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private final TDItem[] fields;
    private final int size;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return Arrays.stream(fields).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        /*
         * If typeAr.length < fieldAr.length, we discard redundant names. Otherwise,
         * we pad nulls names.
         */
        fields = new TDItem[typeAr.length];
        int sum = 0;
        for (int i = 0; i < typeAr.length; i++) {
            if (i >= fieldAr.length) {
                fields[i] = new TDItem(typeAr[i], null);
            } else {
                fields[i] = new TDItem(typeAr[i], fieldAr[i]);
            }
            sum += typeAr[i].getLen();
        }
        size = sum;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        fields = new TDItem[typeAr.length];
        int sum = 0;
        for (int i = 0; i < typeAr.length; i++) {
            fields[i] = new TDItem(typeAr[i], null);
            sum += typeAr[i].getLen();
        }
        size = sum;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i > fields.length)
            throw new NoSuchElementException(
                    String.format("Index out of range, target: %d, length: %d\n",
                            i,
                            fields.length));
        return fields[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i > fields.length)
            throw new NoSuchElementException(
                    String.format("Index out of range, target: %d, length: %d\n",
                            i,
                            fields.length));
        return fields[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].fieldName != null && fields[i].fieldName.equals(name))
                return i;
        }
        throw new NoSuchElementException(String.format("Cannot find field %s in the tuple\n", name));
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] types = new Type[td1.fields.length + td2.fields.length];
        String[] names = new String[td1.fields.length + td2.fields.length];
        for (int i = 0; i < td1.fields.length; i++) {
            types[i] = td1.getFieldType(i);
            names[i] = td1.getFieldName(i);
        }
        for (int i = 0; i < td2.fields.length; i++) {
            types[td1.fields.length + i] = td2.getFieldType(i);
            names[td1.fields.length + i] = td2.getFieldName(i);
        }
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        try {
            TupleDesc opposite = (TupleDesc) o;
            if (opposite.fields.length != fields.length)
                return false;
            for (int i = 0; i < fields.length; i++) {
                if (fields[i].fieldName != opposite.getFieldName(i))
                    return false;
                if (fields[i].fieldType != opposite.getFieldType(i))
                    return false;
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String res = "";
        for (int i = 0; i < fields.length; i++) {
            res += fields[i].toString();
        }
        return res;
    }
}
