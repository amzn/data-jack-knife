package com.amazon.djk.natdb;

import com.amazon.djk.file.FileArgs;
import com.amazon.djk.file.FileSystem;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordIO.IORecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

class BucketAccess {
    private static final Logger logger = LoggerFactory.getLogger(BucketAccess.class);
    private static final String NDB = "ndb";
    private static final String RECORDS = "records";
    private static final String OFFSETS = "offsets";
    private static final String BIN = "bin";
    private final ByteBuffer recordMap;
    private final ByteBuffer offsetMap;
    
	private AtomicBitSet.ClearBitIterator outerIter = null;
    private AtomicBitSet accessedRecs = null;
    private int nextRecNo = 0;
    
    public static BucketAccess create(FileArgs recordFileArgs) throws IOException {
        String fullpath = recordFileArgs.getPath();
        
        File recs = new File(fullpath);
        if (!recs.exists()) {
            throw new FileNotFoundException(fullpath);
        }
        
        File dir = recs.getParentFile();
        String name = recs.getName();
        name = name.replace(RECORDS, OFFSETS).replace(NDB, BIN);
        File offs = new File(dir, name);
        if (!offs.exists()) {
            throw new FileNotFoundException(name);
        }
        
        MappedByteBuffer recordMap = getMap(recs);
        MappedByteBuffer offsetMap = getMap(offs);

        // null accessedRecs because enableOuterAccess sets it before replication.
        return new BucketAccess(recordMap, offsetMap, null);
    }
    
    public static BucketAccess create(int bucketNo, File dbDir) throws IOException {
        String name = FileSystem.getNumberedFileName(RECORDS, bucketNo, NDB);
        File recs = new File(dbDir, name);
        if (!recs.exists()) return null; 
        
        MappedByteBuffer recordMap = getMap(recs);
        name = FileSystem.getNumberedFileName(OFFSETS, bucketNo, BIN);
        MappedByteBuffer offsetMap = getMap(new File(dbDir, name));
        
        return new BucketAccess(recordMap, offsetMap, null);
    }
    
    public BucketAccess replicate() throws IOException {
    	ByteBuffer rmap = recordMap.asReadOnlyBuffer();
    	ByteBuffer omap = offsetMap.asReadOnlyBuffer();
    	return new BucketAccess(rmap, omap, accessedRecs);
    }
    
    /**
     * 
     * @param file
     * @return
     * @throws IOException
     */
    public static MappedByteBuffer getMap(File file) throws IOException {
    	try (RandomAccessFile raf = new RandomAccessFile(file, "r");
    		 FileChannel channel = raf.getChannel(); ) {
    		MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, 0, (int) channel.size());
    		return map;
    	}
    }
    
    private BucketAccess(ByteBuffer recordMap, ByteBuffer offsetMap, AtomicBitSet accessedRecs) throws IOException {
        this.recordMap = recordMap;
        this.offsetMap = offsetMap;
        this.accessedRecs = accessedRecs;
    }
    
    private BucketAccess() {
        this.recordMap = null;
        this.offsetMap = null;
    }

    public static class Poison extends BucketAccess { }
    
    /**
     * IORecord passed in to reduce memory consumption
     * 
     * @param out
     * @return
     * @throws IOException
     */
    public boolean nextUndecoded(IORecord out) throws IOException {
        return getUndecodedValue(nextRecNo++, true, out);
    }
    
    /**
     * binary search the file.
     * 
     * IORecord passed in to reduce memory consumption
     * 
     * @param keyRecord
     * @param valueRecord the untranslated (i.e RecordIO.translate()) value.
     * @return true if the keyRecord is found otherwise false
     * @throws IOException 
     */
    public boolean getUndecodedValue(Record keyRecord, IORecord valueRecord, boolean onlyOnce) throws IOException {
        int loIntPos = 0;
        int hiIntPos = (offsetMap.limit() / 4) - 1; // inclusive
        int midIntPos = -1;
        
        while (loIntPos <= hiIntPos) {
            midIntPos = loIntPos + (hiIntPos - loIntPos) / 2;
            int cmpval = compareTo(keyRecord, midIntPos);
            if (cmpval == 0) {
            	if (accessedRecs != null) {
            		if (onlyOnce && accessedRecs.get(midIntPos)) {
            			return false;
            		}
            		
            		accessedRecs.set(midIntPos);
            	}
            	
            	return getUndecodedValue(midIntPos, false, valueRecord);
            }
            
            else if (cmpval < 0) hiIntPos = midIntPos - 1;
            else loIntPos = midIntPos + 1;
        }
        
        midIntPos = -1; // fail
        return false;
    }

    boolean getUndecodedValue(int intPos, boolean withKey, IORecord valueRecord) throws IOException {
        int offsetOffset = intPos * 4;
        if (offsetOffset >= offsetMap.limit()) return false;
        
        //rec1.reset();
        int keyoff = offsetMap.getInt(offsetOffset); // getInt(bytePos)
        //            [keyFields][compressedValueFields]
        // keyoff ----^
        
        int nextKeyOffOff = (intPos + 1) * 4;
        int reclen = (nextKeyOffOff < offsetMap.limit()) ? // is last entry in offset map ?
                offsetMap.getInt(nextKeyOffOff) - keyoff :
                recordMap.limit() - keyoff;
        
        valueRecord.set(recordMap, keyoff, reclen);
        
        return true;
    }
    
    /**
     * must be called before first keyedAccess and before replicate()
     */
    public void enableOuterAccess() {
    	accessedRecs = new AtomicBitSet(numRecs());
    }

    private int compareTo(Record keyRecord, int midIntPos) {
        int mapRecOff = offsetMap.getInt(midIntPos*4);
        int mapRecLen = recordMap.limit() - mapRecOff;
        byte[] keyBytes = keyRecord.buffer();
        
        int lim = Math.min(keyRecord.length(), mapRecLen);
        int i = 0;
        while (i < lim) {
            byte a = keyBytes[i];
            byte b = recordMap.get(mapRecOff + i);
            if (a != b) {
                return (a & 0xFF) - (b & 0xFF);
            }

            i++;
        }
        
        if (keyRecord.length() <= mapRecLen) return 0; 
        
        // key matched up to mapRecLen, but keyRecord is longer...
        return -1; // or 1?
    }

	public int numRecs() {
		return offsetMap.limit() / 4;
	}

	/**
	 * 
	 * @param withKey
	 * @param undecodedRec
	 * @return
	 * @throws IOException
	 */
	public boolean nextUndecodedOuter(boolean withKey, IORecord undecodedRec) throws IOException {
		if (outerIter == null) {
			outerIter = accessedRecs.clearBitIterator();
		}
		
		int recNo = outerIter.next();
		if (recNo == -1 || recNo == numRecs()) return false;
		
		getUndecodedValue(recNo, withKey, undecodedRec);
		return true;
	}
}