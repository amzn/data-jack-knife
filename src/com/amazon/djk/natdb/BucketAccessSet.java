package com.amazon.djk.natdb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

import com.amazon.djk.misc.Hashing;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordIO;
import com.amazon.djk.record.RecordIO.Direction;
import com.amazon.djk.record.RecordIO.IORecord;

/**
 * one per thread 
 *
 */
public class BucketAccessSet {
	private final BucketAccess[] buckets; // one per thread
	private final DiskEntryDecoder decoder;
	private final RecordIO recordIO;
	private final int numBuckets;
	
	private final IORecord undecodedRec = new IORecord();
    private final Record outRec = new Record();
    private final BlockingQueue<BucketAccess> outerQueue;
    private BucketAccess currOuter = null;
    
	static final ThreadLocal<Map<String,BucketAccess[]>> accessors =
			new ThreadLocal<Map<String,BucketAccess[]>>() {
		@Override protected Map<String,BucketAccess[]> initialValue() {
			return new HashMap<String,BucketAccess[]>();
		}
	};

	/**
	 * 
	 * @param dbDir
	 * @param numBuckets
	 * @param decoder
	 * @return
	 * @throws IOException
	 */
	public static BucketAccessSet create(File dbDir, int numBuckets, DiskEntryDecoder decoder) throws IOException {
		Map<String,BucketAccess[]> accmap = accessors.get();
		BucketAccess[] buckets = accmap.get(dbDir.getAbsolutePath());
		
		if (buckets == null) {
			buckets = new BucketAccess[numBuckets];
		
			for (int i = 0; i < numBuckets; i++) {
				buckets[i] = BucketAccess.create(i, dbDir);
			}
			
			accmap.put(dbDir.getCanonicalPath(), buckets);
		}
		
		return new BucketAccessSet(buckets, decoder, new ArrayBlockingQueue<>(numBuckets + 1)); // +1 = poison 
	}
	
	/**
	 * 
	 * @param buckets
	 * @param decoder
	 * @param outerAccessQueue
	 */
	private BucketAccessSet(BucketAccess[] buckets, DiskEntryDecoder decoder, BlockingQueue<BucketAccess> outerQueue) {
		this.buckets = buckets;
		this.decoder = decoder;
		this.recordIO = decoder.getRecordIO();
		this.outerQueue = outerQueue;
		numBuckets = buckets.length;
	}
	
	/**
	 * 
	 * @param keyRecord
	 * @param onlyOnce if true, only the first access of a given key may return a value, subsequent calls return null
	 * @return
	 * @throws IOException
	 */
	public Record getValue(Record keyMakerMadeRecord, boolean onlyOnce) throws IOException {
		// hashing and sorting took place on stored fids, so translate
        recordIO.translate(keyMakerMadeRecord, Direction.LIVE_TO_STORED);
        int bucketNo = (int)(Hashing.hash63(keyMakerMadeRecord) % numBuckets);
        
        BucketAccess accessor = buckets[bucketNo];
        if (accessor == null) return null;
        
        if (!accessor.getUndecodedValue(keyMakerMadeRecord, undecodedRec, onlyOnce)) {
        	return null;
        }
        
        decoder.decode(undecodedRec, outRec, false);
        return outRec;
	}
	
	public BucketAccessSet replicate() throws IOException {
		BucketAccess[] newBuckets = new BucketAccess[numBuckets];		
		for (int i = 0; i < numBuckets; i++) {
			newBuckets[i] = buckets[i] != null ? buckets[i].replicate() : null;
		}
	
		return new BucketAccessSet(newBuckets, decoder.replicate(), outerQueue);
	}

	public void enableOuterAccess() {
		for (int i = 0; i < numBuckets; i++) {
			BucketAccess bucketAccess = buckets[i];
			if (bucketAccess == null) continue; 
			bucketAccess.enableOuterAccess();
		}
	}
	
	/**
	 * must be called before any data access.  only one thread must call
	 */
	public void prepareOuterAccess() {
		for (int i = 0; i < numBuckets; i++) {
			BucketAccess bucketAccess = buckets[i];
			if (bucketAccess == null) continue; 
			
			outerQueue.add(bucketAccess);
		}
		
		outerQueue.add(new BucketAccess.Poison());
	}

	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	public Record next() throws IOException {
		boolean done = currOuter != null ? currOuter.nextUndecodedOuter(true, undecodedRec) : false; 
		
		while (!done) {
			currOuter = getNextOuterAccess();
			if (currOuter == null) return null;
			done = currOuter.nextUndecodedOuter(true, undecodedRec);
		}
		
        decoder.decode(undecodedRec, outRec, true);
		return outRec;
	}
	
	/**
	 * 
	 * @return
	 */
	private BucketAccess getNextOuterAccess() {
		try {
			BucketAccess access = outerQueue.take();
			if (access instanceof BucketAccess.Poison) {
				outerQueue.put(access); // put it back
				return null;
			}
		
			return access;
		} 
		
		catch (InterruptedException e) {
			return null;
		}
	}
}
