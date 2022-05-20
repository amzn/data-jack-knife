package com.amazon.djk.sort;

import java.io.IOException;

import com.amazon.djk.record.Record;

public abstract class FieldComparator {
    final static int A_RET_ASCENDING = 1;
    final static int A_RET_DESCENDING = -1 * A_RET_ASCENDING;

    protected final int a_ret;
    protected final int b_ret;
    protected final String fieldName;
    
    public FieldComparator(String fieldName, boolean isAscending) {
        a_ret = isAscending ? A_RET_ASCENDING : A_RET_DESCENDING;
        b_ret = a_ret * -1;
        this.fieldName = fieldName;
    }
    
    public abstract int compare(int index_a, int index_b);
    public abstract void addRecord(Record rec) throws IOException;
	public abstract void init(int size);
}
