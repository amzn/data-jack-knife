package com.amazon.djk.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazon.djk.core.RecordSink;
import com.amazon.djk.record.Record;

/**
 * 
 * Primarily for testing.  Collect records into a List. Available only to
 * DJKProcessor.collect();
 *
 */
public class ListSink extends RecordSink {
    List<Record> recs = new ArrayList<>();
	
    public ListSink() throws IOException {
        this(null);
    }
    
    public ListSink(RecordSink root) throws IOException {
        super(root);
    }

    @Override
    public void drain(AtomicBoolean forceDone) throws IOException {
    	super.drain(forceDone);
    	
        while (!forceDone.get()) {
            Record rec = next();
            if (rec == null) {
            	break;
            }
            
            reportSunkRecord(1);
            recs.add(rec.getCopy());
        }
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new ListSink(this);
    }
    
    /**
     * 
     * @return the records collected by this sink
     */
    public List<Record> getRecords() {
        return recs;
    }
}
