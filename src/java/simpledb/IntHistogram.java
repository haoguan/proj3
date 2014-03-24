package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    int numBuckets;
    int min;
    int max;
    int[] histo;
    double inc;
    int numTups = 0;

    int UNDERFLOW = -1;
    int OVERFLOW = -2;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        numBuckets = buckets;
        this.min = min;
        this.max = max;
        initBuckets();
    }

    private void initBuckets() {
        histo = new int[numBuckets];
        // divide minimum mark for each bucket
        double range = Math.ceil((double)(max - min) / (double)numBuckets); // can be a float or double!
        inc = range;
    }

    private int getBucketIdx(int value) {
    	if (value < min) 
    		return UNDERFLOW;
    	if (value > max)
    		return OVERFLOW;

        int idx = (int)((value - min) / inc);
        if (idx >= numBuckets)
        	idx = numBuckets - 1;
        if (idx < 0)
        	idx = 0;
        return idx;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int idx = getBucketIdx(v);
        histo[idx]++;
        numTups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int idx = getBucketIdx(v);
        switch(op) {
            case EQUALS:
            case LIKE:
            	if (idx == UNDERFLOW || idx == OVERFLOW)
            		return 0.0;
            	return computeEqualitySelectivity(idx);
            case GREATER_THAN:
            	if (idx == OVERFLOW)
            		return 0.0;
            	if (idx == UNDERFLOW)
            		return 1.0;
            	return computeGreaterThanSelectivity(false, v, idx);
            case LESS_THAN:
           		if (idx == UNDERFLOW)
           			return 0.0;
           		if (idx == OVERFLOW)
           			return 1.0;
            	return computeLessThanSelectivity(false, v, idx);
            case LESS_THAN_OR_EQ:
           		if (idx == UNDERFLOW)
           			return 0.0;
           		if (idx == OVERFLOW || v == max)
           			return 1.0;
            	return computeLessThanSelectivity(true, v, idx);
            case GREATER_THAN_OR_EQ:
            	if (idx == OVERFLOW)
           			return 0.0;
           		if (idx == UNDERFLOW || v == min)
            		return 1.0;
            	return computeGreaterThanSelectivity(true, v, idx);
            case NOT_EQUALS:
            	if (idx == UNDERFLOW || idx == OVERFLOW)
            		return 1.0;
            	return 1.0 - computeEqualitySelectivity(idx);
            default:
            	System.out.println("WOAH. BAD OP YO.");
        		return -1.0;
        }
    }

    private double computeEqualitySelectivity(int idx) {
    	double width = inc;
    	int height = histo[idx];
    	return (height/width)/numTups;
    }

    private double computeGreaterThanSelectivity(boolean equalTo, int value, int idx) {
    	double width = inc;
    	int height = histo[idx];
    	double total = 0;
    	// find contribution in same bucket
    	double part = ((inc * (idx+1)) - value) / width;
    	if (equalTo)
    		part = ((inc * (idx+1)) - value + 1) / width;
    	total += (height * part);
    	// add up all subsequent buckets
    	for (int i = idx+1; i < numBuckets; i++) {
    		height = histo[i];
    		total += height;
    	}
    	return total/numTups;
    }

    private double computeLessThanSelectivity(boolean equalTo, int value, int idx) {
    	double width = inc;
    	int height = histo[idx];
    	double total = 0;
    	// find contribution in same bucket
    	double frac = (double)height / numTups;
    	double part = (value - (inc * idx)) / width;
    	if (equalTo)
    		part = (value - (inc * idx) + 1) / width;
    	total += (frac * part);
    	// add up all previous buckets
    	for (int i = 0; i < idx; i++) {
    		height = histo[i];
    		total += (double) height / numTups;
    	}
    	return total;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    public int getNumTups() {
    	return numTups;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
