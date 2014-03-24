package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    int tableid;
    int ioCostPerPage;
    HeapFile file;
    Object[] histos;
    Stats[] fieldRanges;
    TupleDesc td;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        file = (HeapFile) Database.getCatalog().getDbFile(tableid);
        this.td = file.getTupleDesc();
        makeStatistics();
    }

    private void makeStatistics() {
        fieldRanges = new Stats[td.numFields()]; // potentially one int histogram for each field.
        histos = new Object[td.numFields()];
        for (int i = 0; i < fieldRanges.length; i++) {
            fieldRanges[i] = new Stats();
        }
        HeapFileIterator it = (HeapFileIterator) file.iterator(new TransactionId());
        try {
            it.open();
        } catch (Exception e) {
            System.out.println("CANNOT OPEN!");
        }

        try {
            generateRanges(it);
        } catch (Exception e) {
            System.out.println("GENERATE RANGES FAILED");
            e.printStackTrace();
        }
        
        try {
            generateHistograms();
        } catch (Exception e) {
            System.out.println("GENERATE HISTOGRAM FAILED");
            e.printStackTrace();
        }
        try {
            storeValues(it);
        } catch (Exception e) {
            System.out.println("Store Values Failed!");
            e.printStackTrace();
        }
    }

    private void generateRanges(HeapFileIterator it) throws Exception {
        it.rewind();
        while (it.hasNext()) {
            Tuple next = it.next();
            // check each field value in tuple.
            for (int i = 0; i < td.numFields(); i++) {
                Field f = next.getField(i);
                // only need to check int, string don't need range!
                if (f.getType() == Type.INT_TYPE) {
                    IntField intF = (IntField) f;
                    int fieldVal = intF.getValue();
                    updateRange(i, fieldVal);
                }
            }
        }
    }

    private void updateRange(int i, int value) {
        Stats s = fieldRanges[i];
        if (value > s.high)
            s.high = value;
        else if (value < s.low) {
            s.low = value;
        }
    }

    private void generateHistograms() throws Exception{
        for (int i = 0; i < td.numFields(); i++) {
            switch(td.getFieldType(i)) {
                case INT_TYPE:
                    histos[i] = new IntHistogram(NUM_HIST_BINS, fieldRanges[i].low, fieldRanges[i].high); // need to figure out low and high.
                    break;
                case STRING_TYPE:
                    histos[i] = new StringHistogram(NUM_HIST_BINS);
                    break;
                default:
                    System.out.println("BAD TD TYPE");
                    break;
            }
        }
    }

    private void storeValues(HeapFileIterator it) throws Exception {
        it.rewind();
        while (it.hasNext()) {
            Tuple next = it.next();
            for (int i = 0; i < td.numFields(); i++) {
                Field f = next.getField(i);
                if (f.getType() == Type.INT_TYPE) {
                    IntField intF = (IntField) f;
                    int val = intF.getValue();
                    IntHistogram ih = (IntHistogram) histos[i];
                    ih.addValue(val);
                }
                else if (f.getType() == Type.STRING_TYPE) {
                    StringField strF = (StringField) f;
                    String str = strF.getValue();
                    StringHistogram sh = (StringHistogram) histos[i];
                    sh.addValue(str);
                }
            }
        }
    }

    private class Stats {
        int high;
        int low;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ioCostPerPage * file.numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        int numTups = totalTuples();
        return (int) (numTups * selectivityFactor);
        // return 0;
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type cType = constant.getType();
        if (cType == Type.INT_TYPE) {
            IntHistogram ih = (IntHistogram) histos[field];
            int val = ((IntField) constant).getValue();
            return ih.estimateSelectivity(op, val);
        }
        else if (cType == Type.STRING_TYPE) {
            StringHistogram sh = (StringHistogram) histos[field];
            String val = ((StringField) constant).getValue();
            return sh.estimateSelectivity(op, val);
        }
        return 0.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        Type f = td.getFieldType(0);
        int numVals = 0;
        if (f == Type.INT_TYPE) {
            IntHistogram ih = (IntHistogram) histos[0];
            numVals = ih.getNumTups();
        }
        else if (f == Type.STRING_TYPE) {
            StringHistogram sh = (StringHistogram) histos[0];
            numVals = sh.getNumTups();
        }
        return numVals;
    }

}
