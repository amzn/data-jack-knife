package com.amazon.djk.source;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazon.djk.core.BaseRecordSource;
import com.amazon.djk.core.Splittable;
import com.amazon.djk.record.Record;

/**
 * 
 * Queue for small number of Records
 * 
 */
public class QueueSource extends BaseRecordSource implements Splittable {
    private final BlockingQueue<Record> queue;

    public QueueSource() {
        queue = new LinkedBlockingQueue<>();
    }
    
    public QueueSource(BlockingQueue<Record> queue) {
        this.queue = queue;
    }
    
    @Override
    public Object split() throws IOException {
        return new QueueSource(queue);
    }
    
	/**
	 * the caller needs to be aware as to whether the record about
	 * to be added is a distinct Record object or part of a stream
	 * of Records from a next() method which tend to return a reference
	 * the same object.  For the latter, use makeCopy=true.
	 * 
	 * @param record
	 * @param makeCopy if true a copy of record is added to the queue
	 * @throws IOException 
	 */
	public void add(Record record, boolean makeCopy) throws IOException {
	    if (makeCopy) {
	        record = record.getCopy();
	    }
		queue.add(record);
	}
	
	public void clear() {
	    queue.clear();
	}
	
	public int size() {
	    return queue.size();
	}
	
	@Override
	public Record next() throws IOException {
		return queue.poll();
	}
}
