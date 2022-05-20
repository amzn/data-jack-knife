package com.amazon.djk.natdb;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Record;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MemDBGroupSink extends MemDBKeyedSink {
	private final AbsentFunction absentFunc;
	private final PresentFunction presentFunc;
    
	public MemDBGroupSink(OpArgs args) throws IOException {
		this(null, args, new ConcurrentHashMap<>());
	}
	
	protected MemDBGroupSink(MemDBGroupSink root, OpArgs args, ConcurrentHashMap<Record,MapRecord> map) throws IOException {
		super(root, args, map);
		Field subName = (Field)args.getParam(MemDBSource.SUBNAME_PARAM);
		absentFunc = new AbsentFunction(subName);
		presentFunc = new PresentFunction(subName);
	}
	
	@Override
	public Object replicate() throws IOException {
		return new MemDBGroupSink(this, args, map);
	}
	 
	@Override
	public void store(Record keyRecord, final Record valueRecord) throws IOException {
		MapRecord subrec = new MapRecord();
		subrec.addFields(valueRecord);
		
		absentFunc.setNewValue(subrec);
		map.computeIfAbsent(keyRecord, absentFunc);
		if (absentFunc.wasAdded()) {
			uniqKeys++;
			numValueBytes += subrec.size();
			numKeyBytes += keyRecord.size();
			capacityBytes += subrec.size() + keyRecord.capacity();
			valueMaxBytes = Math.max(subrec.size(), valueMaxBytes);
		}
		
		else { // was present
			presentFunc.setNewValue(subrec);
			map.computeIfPresent(keyRecord, presentFunc);
			numValueBytes += subrec.size();
			capacityBytes += presentFunc.getDeltaCapacityBytes();
			valueMaxBytes = Math.max(valueMaxBytes, presentFunc.getValueMaxBytes());
		}
	}
	 
	 /**
	  * 
	  *
	  */
	 private static class AbsentFunction implements  Function<Record, MapRecord> {
		 private MapRecord newValue = null;
		 private boolean wasAdded = false;
		 private final Field subname;

		 public AbsentFunction(Field subname) {
			 this.subname = subname;
		 }
		 
		 public void setNewValue(MapRecord newValue) {
			 this.newValue = newValue;
			 wasAdded = false;
		 }
		 
		 public boolean wasAdded() {
			 return wasAdded;
		 }
		 
		 @Override
		 public MapRecord apply(Record key) {
			 MapRecord parent = new MapRecord();
             // FIXME: this shouldn't have to throw
             // since Field already instantiated
			 try {
                parent.addField(subname, newValue);
            } catch (IOException e) {
            }
			 wasAdded = true;
			 return parent;
		 }
	 }
	 
	 /**
	  * 
	  *
	  */
	 private static class PresentFunction implements  BiFunction<Record, MapRecord, MapRecord> {
		 private final Field subname;
		 private MapRecord newValue = null;
		 private int valueMaxBytes = 0;
		 private int deltaCapacityBytes = 0;
		 
		 public PresentFunction(Field subname) {
			 this.subname = subname;
		 }
		 
		 public void setNewValue(MapRecord newValue) {
			 this.newValue = newValue;
		 }
		 
		 public int getValueMaxBytes() {
			 return valueMaxBytes;
		 }
		 
		 /**
		  * 
		  * @return the capacity delta  due to this application. always positive
		  */
		 public int getDeltaCapacityBytes() {
			 return deltaCapacityBytes;
		 }
		 
		 @Override
		 public MapRecord apply(Record key, MapRecord presentValue) {
		     int before = presentValue.capacity();
		     // FIXME: this shouldn't have to throw
             // since Field already instantiated
		     try {
                presentValue.addField(subname, newValue);
            } catch (IOException e) {

            }
		     
		     deltaCapacityBytes = presentValue.capacity() - before;
		     valueMaxBytes = Math.max(valueMaxBytes, presentValue.size());
			 return presentValue;
		 }
	 }
}
