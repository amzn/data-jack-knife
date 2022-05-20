package com.amazon.djk.concurrent;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.KeyMaker;
import com.amazon.djk.record.Record;

import java.io.IOException;

public class RecordCounter {
	public final static String COUNT_FIELD = "count";

	private final OpArgs args;
	private final KeyMaker keyMaker;
	private final boolean retainOthers;
	private final CounterMap<Record> map;
	private final boolean withCount;
	
	public class CounterRecord extends Record {
	    public Record child = null; 
	}
	
	@Override
	public String toString() {
		return keyMaker.toString();
	}
	
	/**
	 * 
	 * @throws IOException
	 */
    public RecordCounter(OpArgs args) throws IOException {
        this(args, new CounterMap<Record>());
	}
	
    public RecordCounter(OpArgs args, CounterMap<Record> map) throws IOException {
        Fields fields = (Fields)args.getArg(UniquePipe.INPUT_FIELDS_ARG);
        keyMaker = new KeyMaker(fields);
        withCount = (Boolean)args.getParam(UniquePipe.COUNT_PARAM);
        retainOthers = (Boolean)args.getParam(UniquePipe.RETAIN_PARAM);
        this.map = map;
        this.args = args;
    }
    
	public long getCount(Record key) {
		return map.getCount(key);
	}
	
	public Object replicate() throws IOException {
	    return new RecordCounter(args, map);
	}
	
	/**
	 * 
	 * @param rec
	 * @throws IOException 
	 */
	public void count(Record rec) throws IOException {
		CounterRecord keyRec = new CounterRecord(); 
		keyMaker.copyTo(rec, keyRec);
		boolean newlyInserted = map.inc(keyRec); 
		if (retainOthers && newlyInserted) { // we will return the child instead of the parent
			keyRec.child = rec.getCopy(); // we retain only onto the first key
		}
	}
	
	public long size() {
		return map.size();
	}
	
	/**
	 * finish counting
	 */
	public void finish() {
		map.finish();
	}
	
	public Record next() throws IOException {
		Record rec = map.next();
		if (rec == null) return null;
		
		if (retainOthers) {
		    rec = ((CounterRecord)rec).child;
		}
		
		if (withCount) {
        	long count = map.getCount((CounterRecord)rec);
        	rec.addField(COUNT_FIELD, count);
        }
    	
		return rec;
	}

	public void reset() {
		map.clear();
	}
}
