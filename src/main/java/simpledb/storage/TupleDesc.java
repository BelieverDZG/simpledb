package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * TupleDesc里Fields的数量
     */
    private int numFields;

    /**
     *
     */
    private TDItem[] tdAr;


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
            this.fieldType = t;
            this.fieldName = n;
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof TDItem) {
                TDItem another = (TDItem) o;
                boolean fieldNameEquals = (another.fieldName == null && this.fieldName == null)
                        || another.fieldName.equals(this.fieldName);
                boolean fieldTypeEquals = another.fieldType.equals(this.fieldType);
                return fieldNameEquals && fieldTypeEquals;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        //return Arrays.asList(tdAr).iterator();
        return new TDItemIterator();
    }

    private class TDItemIterator implements Iterator<TDItem> {

        private int pos = 0;

        @Override
        public boolean hasNext() {
            return tdAr.length > pos;
        }

        @Override
        public TDItem next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return tdAr[pos++];
        }
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
        // some code goes here
        assert typeAr != null;
        assert fieldAr != null;
        if (typeAr.length == 0) {
            throw new IllegalArgumentException();
        }
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("");
        }

        this.numFields = typeAr.length;
        this.tdAr = new TDItem[typeAr.length];

        for (int i = 0; i < typeAr.length; i++) {
            tdAr[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    public TupleDesc(TDItem[] tdItems) {
        if (tdItems == null || tdItems.length == 0) {
            throw new IllegalArgumentException("tuple 描述数组不能为null");
        }
        this.tdAr = tdItems;
        this.numFields = tdItems.length;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields) {
            throw new IllegalArgumentException("索引: " + i + "非法");
        }
        return this.tdAr[i].fieldName;
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
        // some code goes here
        if (i < 0 || i >= numFields) {
            throw new IllegalArgumentException("索引: " + i + "非法");
        }
        return this.tdAr[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException();
        }
        String fieldName;
        // TODO: 17-5-22 improve this,不要使用遍历
        for (int i = 0; i < tdAr.length; i++) {
            if ((fieldName = tdAr[i].fieldName) != null && fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalSize = 0;
        for (TDItem item : tdAr) {
            totalSize += item.fieldType.getLen();
        }
        return totalSize;
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
        // some code goes here
        TDItem[] tdItems1 = td1.tdAr;
        TDItem[] tdItems2 = td2.tdAr;
        int length1 = tdItems1.length;
        int length2 = tdItems2.length;
        TDItem[] resultItems = new TDItem[length1 + length2];
        System.arraycopy(tdItems1, 0, resultItems, 0, length1);
        System.arraycopy(tdItems2, 0, resultItems, length1, length2);
        return new TupleDesc(resultItems);
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
        // some code goes here
        if (this == o) {
            return true;
        }
        if (o instanceof TupleDesc) {
            //当且仅当field数量相同且每个的field的名字和类型都相同时返回true
            TupleDesc another = (TupleDesc) o;
            if (!(another.numFields() == this.numFields())) {
                return false;
            }
            for (int i = 0; i < numFields(); i++) {
                if (!tdAr[i].equals(another.tdAr[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
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
    @Override
    public String toString() {
        // some code goes here
        // some code goes here
        StringBuffer result = new StringBuffer();
        result.append("Fields: ");
        for (TDItem tdItem : tdAr) {
            result.append(tdItem.toString() + ", ");
        }
        result.append(numFields + " Fields in all");
        return result.toString();
    }
}
