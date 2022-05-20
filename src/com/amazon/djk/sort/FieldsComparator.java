package com.amazon.djk.sort;

import com.amazon.djk.record.Record;

import java.io.IOException;

public class FieldsComparator {
    private int[] sortOrds;
    private FieldComparator[] comparators = null;
    private int numRecords;

    public FieldsComparator(SortSpec[] sortSpecs) {
        this.comparators = new FieldComparator[sortSpecs.length];
        
        for (int i = 0; i < sortSpecs.length; i++) {
            SortSpec spec = sortSpecs[i];
            switch(spec.type()) {
        
            case STRING:
                comparators[i] = new StringFieldComparator(spec.fieldName(), spec.isAscending());
                break;

            case LONG:
                comparators[i] = new LongFieldComparator(spec.fieldName(), spec.isAscending());
                break;
                
            case DOUBLE:
                comparators[i] = new DoubleFieldComparator(spec.fieldName(), spec.isAscending());                                
                break;
                
            default:
            }
        }
    }        
    
    public void init(int size) {
        if (sortOrds == null || sortOrds.length < size) {
        	sortOrds = new int[size];
        }
        numRecords = size;
        
        for (FieldComparator comp : comparators) {
        	comp.init(size);
        }
    }
    
    public void addRecord(Record rec) throws IOException {
        for (FieldComparator comp : comparators) {
            comp.addRecord(rec);
        }
    }

    public int[] getOrds() {
        for (int i = 0; i < numRecords; i++) {
            sortOrds[i] = i;            
        }
        
        return sortOrds;
    }
    
    public int compare(int ord_index_a, int ord_index_b) {
        for (FieldComparator comparator : comparators) {
            int comp = comparator.compare(sortOrds[ord_index_a], sortOrds[ord_index_b]);
            if (comp != 0) return comp;
        }
        
        return 0;
    }
}
