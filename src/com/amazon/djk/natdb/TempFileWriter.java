package com.amazon.djk.natdb;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;

/**
 * class for writing temporary sort files to disk 
 */
public class TempFileWriter extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(TempFileWriter.class);
    private final DataOutputStream outStream;
    private final BlockingQueue<RecordFIFO> writeQueue = new SynchronousQueue<>();
    private final int bucketNo;

    public TempFileWriter(File dbDir, int bucketNo) throws IOException {
        this.outStream = getTempOutStream(dbDir, bucketNo);
        this.bucketNo = bucketNo;
    }
    
    private static DataOutputStream getTempOutStream(File dbDir, int bucketNo) throws IOException {
        String name = String.format("temp.%02d", bucketNo);
        OutputStream os = new FileOutputStream(new File(dbDir, name));
        os = new BufferedOutputStream(os, 1024 * 5);
        return new DataOutputStream(os);
    }
    
    public static class PoisonFIFO extends RecordFIFO {
		public PoisonFIFO() throws IOException {
			super();
		}
    }
    
    public void finish() throws InterruptedException, IOException {
        writeQueue.put(new PoisonFIFO());
    }
    
    public void put(RecordFIFO fifo) throws InterruptedException {
        writeQueue.put(fifo);
    }
    
    @Override
    public void run() {
        while (true) {
            try {
                // fifo holds key,value record pairs 
                RecordFIFO fifo = writeQueue.take();
                if (fifo instanceof PoisonFIFO) {
                    break;
                }
                
                // fifo record composition:
                // [keyFields][compressedValueField]
                
                // temp file format:
                // [recLen][keyFields][compressedValueField]
                while (true) {
                    Record rec = fifo.next();
                    if (rec == null) break; // emptied
                    outStream.writeInt(rec.length());
                    outStream.write(rec.buffer(), rec.offset(), rec.length());
                }

            } catch (InterruptedException | IOException e) {
                logger.error("write thread error");
            }
        }
            
        try {
            outStream.close();
        } catch (IOException e) {
            logger.error("write thread error");
        }
        
        logger.info("bucketNo="+bucketNo + " temp file complete");
    }
}
