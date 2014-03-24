package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private String gbfieldname;
    private String afieldname;

    private HashMap<Integer, Integer> groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Aggregation only supports COUNT operator!");
        }
        groups = new HashMap<Integer, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        int val;
        afieldname = tup.getTupleDesc().getFieldName(afield);
        if (gbfield != NO_GROUPING) {
            val = ((IntField)tup.getField(gbfield)).getValue();
            gbfieldname = tup.getTupleDesc().getFieldName(gbfield);
        } else {
            val = (new IntField(NO_GROUPING)).getValue();
        }
        
        if (!groups.containsKey(val)) {
            groups.put(val, 0);
        }
        int total = groups.get(val)+1;
        groups.put(val, total);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        TupleDesc td;
        if (gbfield != NO_GROUPING) {
            td = new TupleDesc(new Type[]{ gbfieldtype, Type.INT_TYPE }, new String[]{ gbfieldname, afieldname });
        } else {
            td = new TupleDesc(new Type[]{ Type.INT_TYPE }, new String[]{ afieldname });
        }
        
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        for (int val : groups.keySet()) {
            int total = groups.get(val);
            Tuple tup = new Tuple(td);
            if (gbfield != NO_GROUPING) {
                tup.setField(0, new IntField(val));
                tup.setField(1, new IntField(total));
            } else {
                tup.setField(0, new IntField(val));
            }
            tuples.add(tup);
        }
        
        return new TupleIterator(td, tuples);
    }

}
