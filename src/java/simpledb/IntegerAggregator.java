package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private String gbfieldname;
    private String afieldname;

    private double aggResult;
    private int aggCount = 0;
    //group
    private HashMap<Object, Double> groups;
    private HashMap<Object, Integer> groupCounts;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groups = new HashMap<Object, Double>();
        groupCounts = new HashMap<Object, Integer>();
        initValues();
    }

    private void initValues() {
        switch (what) {
        case MIN:
            aggResult = Integer.MAX_VALUE;
            break;
        case MAX:
            aggResult = Integer.MIN_VALUE;
            break;
        case SUM:
        case AVG:
        case COUNT:
            aggResult = 0;
            break;
        default:
            System.out.println("INVALID ENTRY!");
        }
        aggCount = 0; //need to reset count too for new group!
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Object key;
        if (gbfield != NO_GROUPING) {
            if (gbfieldtype == Type.INT_TYPE) {
                key = ((IntField)tup.getField(gbfield)).getValue();
                gbfieldname = tup.getTupleDesc().getFieldName(gbfield);
            } else {
                key = ((StringField)tup.getField(gbfield)).getValue();
                gbfieldname = tup.getTupleDesc().getFieldName(gbfield);
            }
        } else {
            key = (new IntField(NO_GROUPING)).getValue();
        }

        int val = ((IntField)tup.getField(afield)).getValue();
        afieldname = tup.getTupleDesc().getFieldName(afield);
        //if contains key already, don't use default value.
        if (groups.containsKey(key)) {
            aggResult = groups.get(key);
            aggCount = groupCounts.get(key);
        } else {
            //reset because new group!
            initValues();
        }
        aggCount++;
        switch (what) {
        case MIN:
            aggResult = Math.min(aggResult, val);
            break;
        case MAX:
            aggResult = Math.max(aggResult, val);
            break;
        case SUM:
            aggResult += val;
            break;
        case AVG:
            //recalculate the average.
            double aggSum = aggResult * (aggCount - 1);
            aggSum += val;
            aggResult = aggSum / (double)aggCount;
            break;
        case COUNT:
            aggResult = aggCount;
            break;
        default:
            System.out.println("INVALID ENTRY!");
        }

        //update group.
        groups.put(key, aggResult);
        groupCounts.put(key, aggCount);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        TupleDesc td = makeTupleDesc();
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        for (Object key : groups.keySet()) {
            Tuple newTup = new Tuple(td);
            int val = groups.get(key).intValue();
            if (gbfield == NO_GROUPING) {
                newTup.setField(0, new IntField(val));
            } else {
                if (gbfieldtype == Type.INT_TYPE) {
                    newTup.setField(0, new IntField((Integer)key));
                } else {
                    newTup.setField(0, new StringField((String)key, gbfieldtype.getLen()));
                }
                newTup.setField(1, new IntField(val));
            }
            tuples.add(newTup);
        }
        return new TupleIterator(td, tuples);
    }

    private TupleDesc makeTupleDesc() {
        if (gbfield == NO_GROUPING) {
            Type[] types = new Type[] {Type.INT_TYPE};
            String[] names = new String[] {afieldname};
            return new TupleDesc(types);
        } else {
            Type[] types = new Type[] {gbfieldtype, Type.INT_TYPE};
            String[] names = new String[] {gbfieldname, afieldname};
            return new TupleDesc(types, names);
        }
    }
}
