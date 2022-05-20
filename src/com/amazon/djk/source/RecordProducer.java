package com.amazon.djk.source;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.core.MinimalRecordSource;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.processor.DJKRunnable;
import com.amazon.djk.record.ThreadDefs;

/**
 * Used with corresponding ConsumerSource 
 *  
 */
public abstract class RecordProducer extends DJKRunnable {
	private static final Logger LOG = LoggerFactory.getLogger(RecordProducer.class);
    protected final BlockingQueue<RecordSource> queue;
    private final AtomicBoolean forceDone;
    private final AtomicBoolean terminated;
    private volatile int numPoisonPuts = 0;
    private volatile IOException nonRecoverableException = null;
    private volatile boolean isDone = false;
    private final AtomicInteger numActiveProducers;
    private final RecordSource poison;
    public static class ProducerPoison extends MinimalRecordSource { }
    private final int numSinkThreads;
    
    /**
     * the constructor for the first producer.  Any additional producers must
     * be instantiated with the OTHER constructor with this object
     * as an argument.
     * 
     * @param queueSize
     * @throws IOException
     */
    public RecordProducer(int queueSize) throws IOException {
    	this.queue = new ArrayBlockingQueue<>(queueSize);
    	this.numActiveProducers = new AtomicInteger(1);
    	poison = new EmptyKeyedSource.EmptySource();
    	this.forceDone = new AtomicBoolean(false);
    	this.terminated = new AtomicBoolean(false);
		this.numSinkThreads = ThreadDefs.get().getNumSinkThreads();
    }
    
    /**
     * the constructor for subsequent producers
     * 
     * @param root
     * @throws IOException
     */
    public RecordProducer(RecordProducer first) throws IOException {
    	this.queue = first.queue;
    	this.numActiveProducers = first.numActiveProducers;
    	this.poison = first.poison;
    	this.forceDone = first.forceDone;
    	this.terminated = first.terminated;
    	numActiveProducers.getAndIncrement();
		this.numSinkThreads = ThreadDefs.get().getNumSinkThreads();
    }
        
    public BlockingQueue<RecordSource> getQueue() {
    	return queue;
    }
    
    public int getNumPoisonPuts() {
        return numPoisonPuts;
    }
    
    public boolean isPutBlocked() {
        return queue.remainingCapacity() == 0;
    }
    
    public RecordSource getPoison() {
    	return poison;
    }
    
    public synchronized IOException getNonRecoverableException() {
    	return nonRecoverableException; 
    }

    /**
     * 
     * @return the next RecordSource to be queued, returns null when done.
     * 
     * @throws Exception
     */
    public abstract RecordSource getNextQueueableSource() throws Exception;
    
    /**
     * called from within Runnable.run() method.  This makes it possible
     * to have long running initialization occur outside of component
     * construction, which makes for more informative reporting.
     * 
     * @throws Exception
     */
    public abstract void initialize() throws Exception;

    /**
     * close the producer
     * 
     * @throws Exception
     */
    public abstract void close() throws IOException;
    
	@Override
	public void innerRun() {
		try {
			initialize();

			while (!forceDone.get()) {
				RecordSource elem = getNextQueueableSource();
				if (elem == null) {
					break;
				}
				
				queue.put(elem);
			}
		} 
		
		catch (Exception e) {
			// consumer logs
            nonRecoverableException = new IOException(e);
            forceDone.set(true);
        } 
		
		finally {
    		if (numActiveProducers.decrementAndGet() != 0) { 
    			isDone = true;
    		    return; // this producer not last
    		}
        	
    		// only poison the queue for the consumers when NOT terminated by them
    		if (!terminated.get()) {
    			try {        			
    				for (int i = 0; i < numSinkThreads; i++) {
        				queue.put(getPoison());
        				numPoisonPuts++;
        			}
        		
    				LOG.info(numPoisonPuts + " poisons added to queue");

    			} catch (Exception e) {
        			LOG.error("", e);
        		}
        	}
    		
    		LOG.info("all producers finished");    
    		isDone = true;
		}
	}

	/**
	 * for outside thread to early terminate from ConsumerSource.close()
	 * i.e. ConsumerSources all shutting down, no need for poison.
	 */
    public void terminate() {
    	terminated.set(true);
    	forceDone.set(true);
    }

	public boolean isDone() {
		return isDone;
	}
}
