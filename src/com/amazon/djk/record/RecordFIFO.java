package com.amazon.djk.record;

import java.io.IOException;

import com.amazon.djk.core.BaseRecordSource;
import com.amazon.djk.processor.FieldDefs;
import com.amazon.djk.record.RecordIO.IORecord;

/**
 * 
 * Memory efficient FIFO RecordSource for storing records
 */
public class RecordFIFO extends BaseRecordSource {
    final IORecord storage = new IORecord();
	private final FieldIterator fields;
	private final Record rec = new Record();
	private boolean nextReady = false;
	private final String childName;
	
	public RecordFIFO(String childName) {
	    this.childName = childName;
	    this.fields = new FieldIterator();
	}
	
	public RecordFIFO() {
	    childName = FieldDefs.INTERNAL_FIELD_NAME;
	    this.fields = new FieldIterator();
	}

    public void add(Record record) throws IOException {
		if (nextReady) {
			throw new RuntimeException("illegal to add records after first call to next()");
		}

		storage.addField(childName, record);
	}
    
    /**
     * 
     * @return the entire FIFO represented as child records of a single parent record 
     * 
     */
    public Record getAsRecord() {
        return storage;
    }
	
	/**
	 * 
	 * @return
	 */
	@Override
	public Record next() throws IOException {
		init();
		if (storage.size() == 0 || !fields.next()) {
			return null;
		}
		
		fields.getValueAsRecord(rec);
		reportSourcedRecord(rec);
		return rec;
	}
	
	private void init() {
		if (nextReady) return;
		fields.init(storage);		
		nextReady = true;
	}

	public long byteSize() {
		return storage.size();
	}
	
	public void reset() {
		nextReady = false;
		storage.reset();
	}

	@Override
	public void close() throws IOException {
		storage.reset();
	}
	
	@Override
	public String toString() {
		return storage.toString();
	}
}
