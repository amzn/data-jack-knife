package com.amazon.djk.concurrent;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * This class is threadsafe!
 *
 */
public class CounterMap<T> {
	private final static int DEFAULT_PRUNE_CHECK_NUM_RECS = 1000000; // 1M
	private final int pruneCheckNumRecs = DEFAULT_PRUNE_CHECK_NUM_RECS;
	private final AtomicLong numRecs = new AtomicLong(0);
	private final ConcurrentHashMap<T,AtomicLong> items = new ConcurrentHashMap<>();
	private final int minTypeCount;
	private final int maxTypes;
	
	private ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
	private CountDownLatch queueReadyLatch = new CountDownLatch(1);
	
	public CounterMap() {
		this(0, -1);
	}
	
	public CounterMap(int minTypeCount, int maxTypes) {
		this.minTypeCount = minTypeCount;
		this.maxTypes = maxTypes;		
	}
	
	/**
	 * 
	 * @param key
	 * @param rec
	 * @return true if this key was newly inserted
	 */
	public boolean inc(T key) {
		boolean wasInserted = false;
		AtomicLong val = items.putIfAbsent(key, new AtomicLong(1));
		if (val != null) {
			val.incrementAndGet();			
		}
		
		else wasInserted = true;
		
		long num = numRecs.incrementAndGet();
		if (num % pruneCheckNumRecs == 0 && maxTypes > 0) {
			if (items.size() > maxTypes){
				prune(minTypeCount);
			}
		}
		
		return wasInserted;
	}
	
	/**
	 * finish counting
	 */
	public synchronized void finish() {
	    if (queueReadyLatch.getCount() == 0) return;
		
		Set<T> set = items.keySet();
    	Iterator<T> iter = set.iterator();
    	
    	while (iter.hasNext()) {
    		queue.add(iter.next());
    	}
    	
    	queueReadyLatch.countDown();
	}
	
	public T next() {
	    try {
            queueReadyLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
		
		return queue.poll();
	}
	
	public long getCount(T key) {
		AtomicLong val = items.get(key);
		if (val == null) return Long.MIN_VALUE;
		return val.get();
	}
	
	public void clear() {
		items.clear();
		queue.clear();
		queueReadyLatch = new CountDownLatch(1);
	}
	
	/**
	 * 
	 * @param minThresh
	 */
	private void prune(int minThresh) {
		Set<T> itemSet = items.keySet();
		Iterator<T> iter = itemSet.iterator();
			
		while (iter.hasNext()) {
			T key = iter.next();
			AtomicLong count = items.get(key);
			if (count.get() < minThresh) {
				iter.remove();
			}
		}
	}

	public long size() {
		return items.mappingCount();
	}
}
