package com.amazon.djk.sort;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.KeyMaker;
import com.amazon.djk.record.Record;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unique for use within sorting.  Allows for:
 * 
 * ... sort:+color,-cost?uniqBy=color
 * 
 * will result in taking the highest cost items by color.
 * 
 */
class SortUniquePipe extends RecordPipe {
	private final Fields fields;
    private final AtomicLong numDiscardedDups;
    private final KeyMaker keyMaker;
    private Record lastUniqueingValue = new Record();
    private Record newUniqueingValue = new Record();
    private long currDups = 0;
    private final long maxDupsToKeep;

    public SortUniquePipe(Fields fields, long dupsToKeep) throws IOException {
        this(null, fields, dupsToKeep);
    }
    
    public SortUniquePipe(SortUniquePipe root, Fields fields, long maxDupsToKeep) throws IOException {
        super(root);
        this.maxDupsToKeep = maxDupsToKeep;
        this.fields = fields;
        keyMaker = new KeyMaker(fields);
        numDiscardedDups = root == null ? new AtomicLong(0) : root.numDiscardedDups;
    }

    @Override
    public String toString() {
    	return fields.toString();
    }
    
    /**
     * 
     * @return the number of duplicates discarded
     */
    public long getNumDups() {
    	return numDiscardedDups.get();
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new SortUniquePipe(this, fields, maxDupsToKeep);
    }
    
    @Override
    public boolean reset() {
        lastUniqueingValue.reset();
        currDups = 0;
        return true;
    }
    
    @Override
    public Record next() throws IOException {
    	while (true) {
    		Record rec = super.next();
    		if (rec == null) return null;

    		// copy the uniquing fields 
    		newUniqueingValue.reset();
    		keyMaker.copyTo(rec, newUniqueingValue);
    		
    		// if the curr record matches the last record
    		if (newUniqueingValue.compareTo(lastUniqueingValue) == 0) {
    			if (currDups >= maxDupsToKeep) {
        		    numDiscardedDups.getAndIncrement();
        		    continue;
    			}

    			currDups++;
    			return rec;
    		}
    		
    		else { // else this rec differs, shuffle the uniquing recs
    		    Record temp = lastUniqueingValue;
    		    lastUniqueingValue = newUniqueingValue;
    		    newUniqueingValue = temp;
    		    currDups = 1;
    			return rec;
    		}
    	}
    }
}
