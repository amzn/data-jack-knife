package com.amazon.djk.keyed;

import java.io.IOException;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.core.MinimalRecordSource;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;

public abstract class KeyedSource extends MinimalRecordSource {
    protected final static String KEYS = "KEYS";
    protected final String[] keyFieldNames;
    protected final OpArgs args;
    private final Fields keyFields;
    
    public KeyedSource(OpArgs args, String[] keyFieldNames) throws IOException {
        this.args = args;
        this.keyFieldNames = keyFieldNames;
        keyFields = new Fields(keyFieldNames);
    }
    
    public KeyedSource(OpArgs args, Fields keyFields) throws IOException {
        this.args = args;
        this.keyFieldNames = keyFields.getFieldNames().toArray(new String[0]);
        this.keyFields = keyFields;
    }
	
    /**
     * used by direct consumer of KeyedSource for replicating the source across
     * threads for indexed methods (e.g. getPayload()) only (not next()).
     * 
     * @return 
     */
    public Object replicateKeyed() throws IOException {
        return null;
    }

    public String[] getKeyFieldNames() {
        return keyFieldNames;
    }
    
    public Fields getKeyFields() {
        return keyFields;
    }
    
    /**
     * provides best guess of number of records.
     * 
     * @return
     */
    public long getNumRecords() {
    	return -1L;
    }
    
    /**
     * 
     * @param key a record containing the key fields
     * @return a record which excludes the key fields
     * @throws IOException
     */
    public abstract Record getValue(Record keyRecord) throws IOException;
}
