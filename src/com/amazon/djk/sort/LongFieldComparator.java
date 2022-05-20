package com.amazon.djk.sort;

import java.io.IOException;

import com.amazon.djk.record.Record;

public class LongFieldComparator extends FieldComparator {
    private long[] values;
    private int idx = 0;
    
    public LongFieldComparator(String fieldName, boolean isAscending) {
        super(fieldName, isAscending);
    }
    
    @Override
    public void init(int size) {
    	if (values == null || values.length < size) {
    		values = new long[size];
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
        values[idx++] = rec.getFirstAsLong(fieldName);
    }
}
