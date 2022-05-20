package com.amazon.djk.natdb;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.amazon.djk.record.Record;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.keyed.KeyedSink;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;
import com.amazon.djk.report.ReportFormats2;

@ReportFormats2(lineFormats={
		"keyMBs=%1.1f valueMBs=%1.1f valueMaxMBs=%1.1f capacityMBs=%1.1f",
})
public class MemDBKeyedSink extends KeyedSink {
	protected final ConcurrentHashMap<Record,MapRecord> map;
    
    @ScalarProgress(name="keyMBs", multiplier=0.000001)
    protected long numKeyBytes = 0;
    @ScalarProgress(name="valueMBs", multiplier=0.000001)
    protected long numValueBytes = 0;
    @ScalarProgress(name="capacityMBs", multiplier=0.000001)
    protected long capacityBytes = 0;
    
    @ScalarProgress(name="uniqKeys")
    protected long uniqKeys = 0;
    
    @ScalarProgress(name="valueMaxMBs", multiplier=0.000001, aggregate=AggType.MAX)
    protected int valueMaxBytes = 0;
    
    private final AbsentFunction func = new AbsentFunction();
    
	public MemDBKeyedSink(OpArgs args) throws IOException {
		this(null, args, new ConcurrentHashMap<>());
	}
	
	protected MemDBKeyedSink(MemDBKeyedSink root, OpArgs args, ConcurrentHashMap<Record,MapRecord> map) throws IOException {
		super(root, args);
		this.map = map;
	}
	
	public ConcurrentHashMap<Record,MapRecord> getMap() {
		return map;
	}
	
	@Override
	public Object replicate() throws IOException {
		return new MemDBKeyedSink(this, args, map);
	}
	 
	@Override
	public void store(Record keyRecord, final Record valueRecord) throws IOException {
		func.setNewValue(valueRecord);
		map.computeIfAbsent(keyRecord, func);
		if (func.wasAdded()) {
			uniqKeys++;
			numValueBytes += func.getValueNumBytes();
			numKeyBytes += keyRecord.size();
			capacityBytes += func.getCapacityBytes();
			valueMaxBytes = Math.max(func.getValueNumBytes(), valueMaxBytes);
		}
	}
	 
	/**
	 * 
	 *
	 */
	private static class AbsentFunction implements  Function<Record, MapRecord> {
		private Record newValue = null;
		private boolean wasAdded = false;
		private int capacityBytes = 0;
		private int valueNumBytes = 0;
		
		public void setNewValue(Record newValue) {
			this.newValue = newValue;
			wasAdded = false;
		}	
		 
		public boolean wasAdded() {
			return wasAdded;
		}
		
		/**
		  * 
		  * @return the capacity bytes due to this application.
		  */
		 public int getCapacityBytes() {
			 return capacityBytes;
		 }
		 
		 public int getValueNumBytes() {
			 return valueNumBytes;
		 }
		 
		@Override
		public MapRecord apply(Record key) {
			MapRecord maprec = new MapRecord();
			maprec.addFields(newValue);
			capacityBytes = newValue.capacity();
			valueNumBytes = newValue.size();
			wasAdded = true;
			return maprec;
		}
	}
}
