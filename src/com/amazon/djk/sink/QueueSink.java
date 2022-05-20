package com.amazon.djk.sink;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazon.djk.core.RecordSink;
import com.amazon.djk.record.Record;

/**
 * 
 * Sink that collects records across threads into a single queue for inspection 
 *
 */
public class QueueSink extends RecordSink {
	private final BlockingQueue<Record> queue;
	
    public QueueSink() throws IOException {
        this(null, new LinkedBlockingQueue<>());
    }
    
    public QueueSink(RecordSink root, BlockingQueue<Record> queue) throws IOException {
        super(root);
        this.queue = queue;
    }

    @Override
    public void drain(AtomicBoolean forceDone) throws IOException {
    	super.drain(forceDone);
    	
        while (!forceDone.get()) {
            Record rec = super.next();
            if (rec == null) break;
            
            queue.add(rec.getCopy());
            reportSunkRecord(1);
        }
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new QueueSink(this, queue);
    }

    public List<Record> getRecords() {
    	Record[] array = queue.toArray(new Record[0]);
    	return Arrays.asList(array);
    }
}
