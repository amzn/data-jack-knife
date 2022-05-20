package com.amazon.djk.core;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.amazon.djk.record.Record;
import com.amazon.djk.report.ElapsedTime;
import com.amazon.djk.report.ProgressData;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;

/**
 * 
 */
@ReportFormats(lineFormats={
        //https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html
		"<elapsed>%s sinkThreads=%d activeThreads=%d recsSunk=%,d",
})
public class RecordSink extends RecordPipe {
    public final static String NUM_SINK_THREADS_NAME = "sinkThreads";
    public final static String NUM_ACTIVE_THREADS_NAME = "activeThreads";
    public final static String NUM_RECORDS_SUNK_NAME = "recsSunk";
    /**
     * replica constructor 
     * @param root
     * @throws IOException 
     */
	public RecordSink(RecordSink root) throws IOException {
        super(root);
        recsSunk = root != null ? root.recsSunk : new AtomicLong(0); 
    }

    @ScalarProgress(name=NUM_RECORDS_SUNK_NAME, aggregate=AggType.NONE)
	private final AtomicLong recsSunk;
	
	@ScalarProgress(name=NUM_SINK_THREADS_NAME)
    private final int sinkThreads = 1;
	
	@ScalarProgress(name=NUM_ACTIVE_THREADS_NAME, aggregate=AggType.NONE)
    private volatile int activeThreads = 0;
	
	@ScalarProgress(name="elapsed")
	private final ElapsedTime elapsedTime = new ElapsedTime();
	
	private long numStrandRecsSunk = 0;
	
	@Override
	public ProgressData getProgressData() {
		activeThreads = getNumActiveInstances();
		return new ProgressData(this);
	}
	
	@Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;

        return rec;
    }
    
	@Override
	public void close() throws IOException {
		super.close();
		elapsedTime.stop();
	}
	
    /**
     * 
     * @return the total number of records sunk across all threads
     */
    public long totalRecsSunk() {
    	return recsSunk.get();
    }
    
    /**
     * 
     * @return the number of records sunk by this thread
     */
    public long getThreadRecsSunk() {
    	return numStrandRecsSunk;
    }
    
    @Override
    // sinks cannot be reset (nor should this ever be called)
    public final boolean reset() {
		return false;
	}
    
    /**
     * increment the number of records sunk by this sink.
     * @param numSunk
     */
    public void reportSunkRecord(long numSunk) {
    	numStrandRecsSunk += numSunk;
        recsSunk.addAndGet(numSunk);
    }
	
	/**
     * super.drain(forceDone) should be called in extending classes immediately within their overriding implementation
     * 
     * @throws IOException
     */
    public void drain(AtomicBoolean forceDone) throws IOException {
    	elapsedTime.start();
    }
}
