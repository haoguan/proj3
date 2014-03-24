package simpledb;

import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {

    HeapPage page;
    Iterator<Tuple> tuples;

    public HeapPageIterator(HeapPage hp) {
        page = hp;
        ArrayList<Tuple> tempTuples = new ArrayList<Tuple>();
        for (int i = 0; i < hp.tuples.length; i++) {
            if (hp.tuples[i] != null) {
                tempTuples.add(hp.tuples[i]);
            }
        }
        tuples = tempTuples.iterator();
    }

    public boolean hasNext() {
    	return tuples.hasNext();
    }

    public Tuple next() {
    	return tuples.next();
    }

    public void remove() {
    	throw new UnsupportedOperationException("not allowed to remove!");
    }
}