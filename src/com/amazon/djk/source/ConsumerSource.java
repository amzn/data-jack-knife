package com.amazon.djk.source;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.core.BaseRecordSource;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.core.Splittable;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.PercentProgress;
import com.amazon.djk.report.ProgressData;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
/**
 * Used with corresponding RecordProducer 
 *
 */
@ReportFormats2(headerFormat="<url>%s", 
	lineFormats={"sourceThreads=%d activeThreads=%d",
	             "puts(blocked=<blockedPuts>%d (<percentBlockedPuts>%2.1f%%) poison=<poisonPuts>%d) takes(blocked=<blockedTakes>%d (<percentBlockedTakes>%2.1f%%) poison=<poisonTakes>%d) Q=<QDepth>%d/<QSize>%d"})
public abstract class ConsumerSource extends BaseRecordSource implements Splittable {
	private static final Logger LOG = LoggerFactory.getLogger(ConsumerSource.class);
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(ConsumerSource.class.getSimpleName() + "-%d").build();
	
    @ScalarProgress(name="url")
	protected final String url;
    
    protected final List<RecordProducer> producers;
    private final BlockingQueue<RecordSource> queue;
    private final AtomicInteger numActiveConsumers;
    
    private final ExecutorService producerPool;
    
    // no agg, num producers
    @ScalarProgress(name = "sourceThreads", aggregate=AggType.NONE)
	private final int numProducers;
    
    // no agg, numActive producers
    @ScalarProgress(name = "activeThreads", aggregate=AggType.NONE)
	private volatile int numActiveProducers = 0;
        
    // no agg, calculated across 
    @ScalarProgress(name="blockedPuts", aggregate=AggType.NONE)
    private volatile int blockedPuts = 0;

    // aggregate across these numSink sources
    @ScalarProgress(name="blockedTakes")
    private volatile int blockedTakes = 0;
    
    // aggregate across these numSink sources
    @ScalarProgress(name="poisonTakes")
    private volatile int numPoisonTakes = 0;
    
    // no agg, calculated across enqueuers
    @ScalarProgress(name="poisonPuts", aggregate=AggType.NONE)
    private volatile int numPoisonPuts = 0;

    // aggregated across threads
    @ScalarProgress(name="numTakeSamples")
    private volatile long numTakeSamples = 0;
    
    // aggregated across threads
    @PercentProgress(denominatorAnnotation = "numTakeSamples", name = "percentBlockedTakes")
    private volatile long totalSampleBlockedTakes = 0;

    // NOT aggregated
    @ScalarProgress(name="numPutSamples", aggregate=AggType.NONE)
    private volatile long sampleBlockedPutsDenom = 0;
    
    @PercentProgress(denominatorAnnotation = "numPutSamples", name = "percentBlockedPuts", aggregate=AggType.NONE)
    private volatile long totalSampleBlockedPuts = 0;
    
    @ScalarProgress(name="QDepth", aggregate=AggType.NONE)
    private volatile int queueDepth;
    
    @ScalarProgress(name="QSize", aggregate=AggType.NONE)
    private final int queueCapacity;
    
    private final boolean isRoot;
    private final RecordSource poison;
    
    private RecordSource currSource = null;
    
    /**
     * base constructor
     * 
     * @param opArgs
     * @param query
     * @throws IOException
     */
    public ConsumerSource(String url, List<RecordProducer> producers) throws IOException {
        numActiveConsumers = new AtomicInteger(1);
        this.producers = producers;
        this.url = url;

        // all producers have the same queue
        poison = producers.get(0).getPoison(); 
        queue = producers.get(0).getQueue();
        queueCapacity = queue.remainingCapacity();
        
        // create an executor pool for the producers
        producerPool = Executors.newFixedThreadPool(producers.size(), threadFactory);
        for (RecordProducer producer : producers) {
            producerPool.execute(producer);        	
        }

        this.numProducers = producers.size();
        isRoot = true;
    }
    
    /**
     * split constructor
     * @param root
     * @throws IOException
     */
    protected ConsumerSource(ConsumerSource root) throws IOException {
        this.producers = root.producers;
        this.url = root.url;
        this.numActiveConsumers = root.numActiveConsumers;
        numActiveConsumers.getAndIncrement();
        this.queue = root.queue;
        this.queueCapacity = root.queueCapacity;
        this.producerPool = root.producerPool;
        this.poison = root.poison;
        this.numProducers = root.numProducers;
        isRoot = false;
    }
    
    @Override
    public ProgressData getProgressData() {
    	blockedPuts = 0;
    	blockedTakes = 0;
    	numPoisonPuts = 0;
    	numActiveProducers = 0;
    	
    	for (RecordProducer producer : producers) {
    		if (producer.isPutBlocked()) blockedPuts++;
    		numActiveProducers += producer.isDone() ? 0 : 1;
    		numPoisonPuts += producer.getNumPoisonPuts();
    	}
    	
        numTakeSamples++; // aggregated
        blockedTakes = isTakeBlocked() ? 1 : 0; // aggregated
        totalSampleBlockedTakes += blockedTakes; // aggregated

        sampleBlockedPutsDenom += numActiveProducers; // not aggregated
        totalSampleBlockedPuts += blockedPuts; // not aggregated
        
        queueDepth = queue.size();
        
        return new ProgressData(this);
    }
    
    @Override
    public void close() throws IOException {
    	super.close();
    	
    	numActiveConsumers.getAndDecrement();
    	if (numActiveConsumers.get() != 0) return;

    	// at this point, we are the last source
    	// next() will not be called again
    	
    	// possible states:
    	// 1) poison pills retrieved from queue for all consumers
    	//   (due to all data exhausted or exception in producer)
    	// 2) early termination due to e.g. 'head'
    	//

    	// in case of early termination where producers still active,
    	// terminate and keep the queue clear for not-yet-put sources to fit
    	while (!allProducersDone()) {
    		producers.get(0).terminate();  // terminates all producer loops.
    		queue.clear();
    		sleep(5);
    	}
		queue.clear();
    	
        LOG.info("producers terminated");

        producerPool.shutdown();		
        while (!producerPool.isTerminated()) {
            try {
                if (!producerPool.awaitTermination(60, TimeUnit.SECONDS)) {
                	LOG.warn("producer pool termination is taking too much time");
                }
            } 
	
            catch (InterruptedException e) {
                LOG.error("", e);
            }
        }

        // make read thread exception throw write thread exception
        
        for (RecordProducer producer : producers) {
        	if (producer.getNonRecoverableException() != null) {
        		throw new IOException(producer.getNonRecoverableException());
        	}
    	}
    }

    private void sleep(long millis) {
    	try {
			Thread.sleep(millis);
		} catch (InterruptedException e) { 
			LOG.warn("sleep interruption");
		}
    }
    
    private boolean isTakeBlocked() {
    	return queueCapacity == queue.remainingCapacity();
    }
    
    /**
     * 
     * @return
     * @throws Exception
     */
    private RecordSource getSourceFromQueue() throws Exception {
		RecordSource queueElem = queue.take();

		if (queueElem == poison) {
			numPoisonTakes++;
			
			Exception e = getProducerException();
    		if (e != null) {
        		throw e;
        	}    			
			
			return null;
		}
		
		return queueElem;
    }

    /**
     * 
     * @return 
     */
    private Exception getProducerException() {
		// only the root throws the producing thread's exception
    	if (!isRoot) return null;
    	
        for (RecordProducer producer : producers) {
        	if (producer.getNonRecoverableException() != null) {
        		return producer.getNonRecoverableException();
        	}
        }
        
        return null;
    }
    
    @Override
    public Record next() throws IOException {
    	try {
    		Record rec = currSource != null ? currSource.next() : null; 
    		while (rec == null) {
    			currSource = getSourceFromQueue();
    			if (currSource == null) return null;
    			rec = currSource.next();
    		}
		
    		reportSourcedRecord(rec);
		
    		return rec;
    	}

    	catch (Exception e) {
    		// sink thread logs
            throw new IOException(e);
    	}
    }
    
    private boolean allProducersDone() {
    	for (RecordProducer producer : producers) {
    		if (!producer.isDone()) return false;
    	}
    	
    	return true;
    }
    
    public List<RecordProducer> getProducers() {
    	return producers;
    }
}
