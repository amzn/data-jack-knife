package com.amazon.djk.stats;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordIO.IORecord;

/**
 * 
 */
public abstract class FieldStats<T> {
    private final String fieldName;
    protected final AtomicLong numValues = new AtomicLong(0);
	protected final ConcurrentHashMap<T,AtomicLong> items = new ConcurrentHashMap<>();
    private final long maxHistogramPoints;
    private final long minPointCount;
	
	public FieldStats(String fieldName, long maxHistogramPoints, long minPointCount) {
	    this.fieldName = fieldName;
	    this.maxHistogramPoints = maxHistogramPoints;
	    this.minPointCount = minPointCount;
	}
	
	/**
	 * 
	 * @param key
	 * @param rec
	 */
    public void count(T key) {
	    numValues.getAndIncrement();
		AtomicLong val = items.putIfAbsent(key, new AtomicLong(1));
		if (val != null) {
			val.incrementAndGet();
		}
	}
	
	public abstract Record getDataPointAsRecord(T value, AtomicLong count) throws IOException;

	public abstract String fieldType();
	
	public Record getNumericalStatsAsRecord() throws IOException {
	    return null;
	}

	public Record getAsRecord(long numRecs, String instance) throws IOException {
	    // IORecord allows resizing since we could be adding thousands of subrecords 
	    IORecord rec = new IORecord(); 
	    rec.addField(StatFields.STATS_FIELD, fieldName);
	    rec.addField(StatFields.STATS_TYPE, fieldType());
	    rec.addField(StatFields.INSTANCE_FIELD, instance);
	    rec.addField(StatFields.STATS_NUM_RECS, numRecs);
	    rec.addField(StatFields.STATS_NUM_VALUES, numValues.get());
	    rec.addField(StatFields.STATS_NUM_UNIQUE_VALUES, (long)items.size());

	    List<T> valueCountDescending = Collections.list(items.keys());
	    Collections.sort(valueCountDescending, new Comparator<T>() {
	        public int compare(T a, T b) {
	        	AtomicLong aCount = items.get(a);
	        	AtomicLong bCount = items.get(b);
	        	return aCount.get() == bCount.get() ? 0 :
	        		aCount.get() > bCount.get() ? -1 : 1;
	        }
	    });
	        
	    Record numerical = getNumericalStatsAsRecord();
	    if (numerical != null) {
	        rec.addFields(numerical);
	    }

	    /**
	     * resizing a record for thousands of sub-recs is hugely expensive
	     * below makes this more efficient since we know how many inserts
	     */
	    int numPoints = 0;
	    int checkNumResizes = 5;
	    for (T value : valueCountDescending) {
	        AtomicLong count = items.get(value);
	        if (count.get() < minPointCount) continue;
	        
            rec.addField(StatFields.STATS_DATA_CHILD, getDataPointAsRecord(value, items.get(value)));
            if (++numPoints == maxHistogramPoints) break;

            if (rec.numResizes() == checkNumResizes) {
            	double recsLeft = valueCountDescending.size() - numPoints;
                double mult =  recsLeft / (double)numPoints;
                // max of 256 MB on estimated resize
                int newSize = Math.min((int)(rec.size() * mult), 1024 * 1024 * 256);
                rec.resize(newSize);
            	checkNumResizes = rec.numResizes() + 5;
            }
	    }
	    
	    return rec;
	}
}
