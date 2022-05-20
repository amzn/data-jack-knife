package com.amazon.djk.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helps synchronize across pipe instances.
 * 
 */
public class IsLastSynchronizer {
	private final AtomicInteger numArrived = new AtomicInteger(0);
	private final RecordPipe pipe;
	
	public IsLastSynchronizer(RecordPipe pipe) {
	    this.pipe = pipe;
	}
	/**
	 * 
	 * @return true if the arriving thread is the last thread to arrive here.
	 * @throws InterruptedException
	 */
	public boolean arriveAndIsLast() {
		int arrived = numArrived.incrementAndGet();
		return arrived == pipe.getNumInstances();
	}
}
