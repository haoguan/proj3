package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;

        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        public String getFieldName() {
            return fieldName;
        }

        public Type getFieldType() {
            return fieldType;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        // return null;
        return (Iterator<TDItem>)TDItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
    * Stores all the TDItems
    * */
    ArrayList<TDItem> TDItems = null;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        TDItems = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        TDItems = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItems.add(new TDItem(typeAr[i], ""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        // return 0;
        return TDItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= TDItems.size())
            throw new NoSuchElementException();
        return TDItems.get(i).getFieldName();
        // return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= TDItems.size())
            throw new NoSuchElementException();
        return TDItems.get(i).getFieldType();
        // return null;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < TDItems.size(); i++) {
            TDItem itm = TDItems.get(i);
            if (itm.getFieldName().equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        // return 0;
        int byteCount = 0;
        for (int i = 0; i < TDItems.size(); i++) {
            TDItem itm = TDItems.get(i);
            byteCount += itm.getFieldType().getLen();
        }
        return byteCount;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] newTypes = new Type[td1.numFields() + td2.numFields()];
        String[] newNames = new String[td1.numFields() + td2.numFields()];
        TDItem[] first = (TDItem[])td1.getTuples().toArray(new TDItem[td1.getTuples().size()]);
        TDItem[] second = (TDItem[])td2.getTuples().toArray(new TDItem[td2.getTuples().size()]);
        for (int i = 0; i < first.length; i++) {
            newTypes[i] = first[i].getFieldType();
            newNames[i] = first[i].getFieldName();
        }
        int firstLen = first.length;
        for (int j = 0; j < second.length; j++) {
            newTypes[firstLen + j] = second[j].getFieldType();
            newNames[firstLen + j] = second[j].getFieldName();
        }
        return new TupleDesc(newTypes, newNames);
        // return null;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc) {
            TupleDesc other = (TupleDesc) o;
            if (other.getSize() != this.getSize())
                return false;
            for (int i = 0; i < other.numFields(); i++) {
                if (!this.TDItems.get(i).getFieldType().equals(
                    other.getTuples().get(i).getFieldType())) {
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
    public String toString() {
        // some code goes here
        String ret = "";
        for (int i = 0; i < TDItems.size() - 1; i++) {
            ret += TDItems.get(i).toString() + ", ";
        }
        ret += TDItems.get(TDItems.size() - 1).toString();
        return ret;
        // return "";
    }

    public ArrayList<TDItem> getTuples() {
        return TDItems;
    }
}
