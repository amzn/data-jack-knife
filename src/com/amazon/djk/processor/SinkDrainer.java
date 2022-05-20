package com.amazon.djk.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.core.RecordSink;
import com.amazon.djk.record.ThreadDefs;

public class SinkDrainer extends DJKRunnable {
	private static final Logger LOG = LoggerFactory.getLogger(SinkDrainer.class);
	private final RecordSink sink;
	private volatile boolean done = false;
	private IOException runException = null;
	private final AtomicBoolean forceDone;
	
	public boolean isDone() {
		return done || forceDone.get();
	}
	
	public IOException getException(){
	    return runException;
	}
	
	private SinkDrainer(RecordSink sink, AtomicBoolean forceDone) throws IOException {
		this.sink = sink;
		this.forceDone = forceDone;
	}
	
	@Override
	public void innerRun() {
		try {
			sink.drain(forceDone);
			if (forceDone.get()) {
				LOG.info("drain forced quit");
			}
		}
		
        catch (Exception e) {
            LOG.error("sink.drain() exception", e);
        	runException = (e instanceof IOException) ? (IOException)e : new IOException(e);
        	forceDone.set(true);
		}
		
		finally {
		    try {
		        sink.close();
		    }
		    
	         catch (Exception e) {
	        	 LOG.error("sink.close() exception", e);
	        	 runException = (e instanceof IOException) ? (IOException)e : new IOException(e);
	        	 forceDone.set(true);
	         }
		    
        	done = true;
		}
	}
	
	/**
	 * 
	 * @param theSink
	 * @return
	 * @throws IOException
	 */
	public static List<SinkDrainer> getDrainers(RecordSink theSink, AtomicBoolean globalForceDone) throws IOException {
	    List<SinkDrainer> workers = new ArrayList<SinkDrainer>();
	    
        // add the root sink                                                                                                                                                                                                                                                                                                         
	    SinkDrainer worker = new SinkDrainer(theSink, globalForceDone);
	    workers.add(worker);

        int numSinkThreads = ThreadDefs.get().getNumSinkThreads();
        // - 1 (since root is one already)                                                                                                                                                                                                                                                              
        for (int i = 0; i < numSinkThreads - 1; i++) {
            RecordSink threadSink = (RecordSink)theSink.getStrand();
            if (threadSink == null) break;

            worker = new SinkDrainer(threadSink, globalForceDone);
            workers.add(worker);
        }

        return workers;
    }

	public RecordSink getSink() {
		return sink;
	}
}
