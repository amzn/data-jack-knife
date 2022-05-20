package com.amazon.djk.sort;

import java.io.IOException;

import com.amazon.djk.record.Record;

public class DoubleFieldComparator extends FieldComparator {
    private double[] values;
    private int idx = 0;
    
    public DoubleFieldComparator(String fieldName, boolean isAscending) {
        super(fieldName, isAscending);
    }
    
    @Override
    public void init(int size) {
    	if (values == null || values.length < size) {
    		values = new double[size];
    	}
    	
    	idx = 0;
    }
    
    @Override
    public int compare(int index_a, int index_b) {
        return values[index_a] > values[index_b] ?
                a_ret : values[index_a] == values[index_b] ?
                        0 : b_ret;
    }
    
    @Override
    public void addRecord(Record rec) throws IOException {
        values[idx++] = rec.getFirstAsDouble(fieldName); 
    }
}
