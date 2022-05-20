package com.amazon.djk.sort;

import java.io.IOException;

import com.amazon.djk.record.BytesRef;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.UTF8BytesRef;

public class StringFieldComparator extends FieldComparator {
    private UTF8BytesRef[] utf8Refs;
    private int idx = 0;
    
    public StringFieldComparator(String fieldName, boolean isAscending) {
        super(fieldName, isAscending);
    }
    
    @Override
    public void init(int size) {
    	if (utf8Refs == null || utf8Refs.length < size) {
    		utf8Refs = new UTF8BytesRef[size];
    	}
    	
    	idx = 0;
    }
    
    /**
     * the stringRef[index]'s point here upon entry
     * 
     *        |<-----------length --------->
     * offset-v
     * |fid(2)|type(1)|strlen(4)|utf8string |
     * 
     * where 
     */
    @Override
    public int compare(int index_a, int index_b) {
        BytesRef a = utf8Refs[index_a];
        BytesRef b = utf8Refs[index_b];
        return a.compareTo(b) * a_ret;
    }

    @Override
    public void addRecord(Record rec) throws IOException {
        UTF8BytesRef curr = utf8Refs[idx];
        if (curr == null) {
            curr = new UTF8BytesRef();
        }

        utf8Refs[idx++] = curr;
        rec.getFirstAsUTF8BytesRef(fieldName, curr);
    }
}
