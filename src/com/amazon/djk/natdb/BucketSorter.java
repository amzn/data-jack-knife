package com.amazon.djk.natdb;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.file.FileSystem;
import com.amazon.djk.processor.FieldDefs;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.FieldType;
import com.amazon.djk.record.RecordIO.IOBytes;
import com.amazon.djk.record.RecordIO.IORecord;
import com.amazon.djk.record.ThreadDefs;
import com.amazon.djk.record.VarLenNumberHelp;

public class BucketSorter {
    private static final Logger logger = LoggerFactory.getLogger(BucketSorter.class);
    private final static float MEMORY_OVERHEAD_FACTOR = 1.1F;
    private final File dbDir;
    private final List<SortElem> elems = new ArrayList<>();
    //private final int numKeyFields;
    private final boolean onePerKey;
    private final Field groupOut;
    private final ByteBuffer buffer;
    private final int bucketNo;
    private final boolean addCount;
    
    private int maxGroupUncompressedByteSize = 0;
    private int maxGroupSize = 0;
    private int numWritten = 0;
    
    public int getMaxGroupUncompressedByteSize() {
        return maxGroupUncompressedByteSize;
    }
    
    public int getMaxGroupSize() {
        return maxGroupSize;
    }

    public int getNumWritten() {
        return numWritten;
    }
    /**
     * 
     * @param dbDir
     * @param bucketNo
     * @param groupOut
     * @param useCompression
     * @throws IOException
     */
    public BucketSorter(File dbDir, Field groupOut, int numKeyFields, boolean addCount, int bucketNo) throws IOException {
        logger.debug("loading bucketNo="+bucketNo);
        this.dbDir = dbDir;
        this.groupOut = groupOut;
        this.addCount = addCount;  
        this.bucketNo = bucketNo;
        
        long maxSize = getMaxTempFileSize(dbDir);
        if (maxSize > Integer.MAX_VALUE) {
            // find out right away
            throw new IOException("implementation does not handle file sizes > 2GB");
        }
        onePerKey = groupOut.getName().equals(MemDBSource.NO_GROUP);
        
        String name = String.format("temp.%02d", bucketNo);
        File file = new File(dbDir, name);
        int fileLen = (int)file.length();
        buffer = BucketAccess.getMap(file);

        IORecord inrec = new IORecord();
        // temp record format:
        // [reclen][keyFields][compressedValueField]
        
        logger.debug("bucketNo="+bucketNo + "mapped");
        
        FieldIterator fiter = new FieldIterator();
        
        buffer.position(0);
        while (buffer.position() < buffer.limit()) {
            int recLen = buffer.getInt();
            
            int keyOff = buffer.position();
            inrec.set(buffer, buffer.position(), recLen);
            fiter.init(inrec);
            
            for (int i = 0; i < numKeyFields; i++) {
                if (!fiter.next()) {
                    throw new IOException("format error");
                }
            }
            
            int keyEnd = keyOff + fiter.offset() + fiter.length();
            
            // compressed value field
            fiter.next();
            int valEnd = keyOff + fiter.offset() + fiter.length();

            elems.add(new SortElem(keyOff, keyEnd-keyOff, valEnd-keyEnd));
            buffer.position(keyOff + recLen);
        }
        
        logger.debug("bucketNo="+bucketNo + " loaded");
    }
    
    /**
     * sort the temp files for a bucket
     * 
     * @throws IOException
     */
    public void sort() throws IOException {
        Collections.sort(elems);
    }
    
    private DataOutputStream getFinalOutStream(String name, int buckeNo, String suffix) throws IOException {
        String file = FileSystem.getNumberedFileName(name, bucketNo, suffix);
        OutputStream os = new FileOutputStream(new File(dbDir, file));
        os = new BufferedOutputStream(os, 1024 * 1024 * 1);
        return new DataOutputStream(os);
    }
    
    /**
     * writes the sorted records to disk
     * 
     * @return the number of records written
     * @throws IOException
     */
    public void write() throws IOException {
        numWritten = 0;
        maxGroupSize = 0;
        maxGroupUncompressedByteSize = 0;
        
        if (elems.size() == 0) {
            // delete temp files
            String name = String.format("temp.%02d", bucketNo);
            File tempFile = new File(dbDir, name);
            tempFile.delete();
            return;
        }
        
        DataOutputStream stream = getFinalOutStream("records", bucketNo, "ndb");
        DataOutputStream offStream = getFinalOutStream("offsets", bucketNo, "bin");

        numWritten = 0;
        maxGroupSize = 0;
        SortElem curr = null;
        int i = 0;
        IORecord valuesRec = new IORecord();
        IOBytes tempVal = new IOBytes();
        
        // get the max required memory so we don't spend resources growing
        maxGroupUncompressedByteSize = calculateGroupMaxUncompressedByteSize();
        valuesRec.resize((int)(maxGroupUncompressedByteSize * MEMORY_OVERHEAD_FACTOR));
        tempVal.resize((int)(maxGroupUncompressedByteSize * MEMORY_OVERHEAD_FACTOR));

        // so the count field matches the group field name, e.g. child, childCount
        // if mapDB where we have no child, use origCount
        String countField = String.format("%sCount", onePerKey ? "orig" : groupOut.getName());
        
        while (i <  elems.size()) {
            curr = elems.get(i);

            int groupSize = getGroupSize(elems, i);
            maxGroupSize = Math.max(maxGroupSize, groupSize);
            
            // format to disk:
            // [keyFields][compressedValueField]
            offStream.writeInt(stream.size()); // offset to beginning of key fields
            
            curr.writeKeyField(stream);
            valuesRec.resetButKeepCapacity();
            if (addCount) {
                valuesRec.addField(countField, groupSize);
            }
            
            if (onePerKey) {
                tempVal.resetButKeepCapacity();
                elems.get(i).writeValueBytesTo(tempVal);
                ThreadDefs.get().inflate(tempVal, valuesRec);
            }

            else {
                for (int j = 0; j < groupSize; j++) {
                    tempVal.resetButKeepCapacity();
                    elems.get(i+j).writeValueBytesTo(tempVal);
                    valuesRec.addBytesAsRecordField(groupOut, tempVal, true);
                }
            }

            // AWKWARD fragment that must stay in sync with Record
            // and FieldIterator mechanics. This is done in order to 
            // not have to compose this in a Record (in memory)
            ThreadDefs.get().deflate(valuesRec, tempVal);
            stream.writeShort(FieldDefs.INTERNAL_FIELD_ID);
            stream.writeByte((byte)FieldType.BYTES_ID); // type
            VarLenNumberHelp.writeVarLenUnsignedInt(stream, tempVal.length()); // payload len
            stream.write(tempVal.buffer(), 0, tempVal.length());
            // end fragment
            
            numWritten++;
            i += groupSize;
        }

        stream.close();
        offStream.close();
        
        // delete temp files
        String name = String.format("temp.%02d", bucketNo);
        File tempFile = new File(dbDir, name);
        tempFile.delete();
    }
    
    /**
     * 
     * @return the memory needed to hold the biggest group in bytes  
     * @throws IOException 
     */
    private int calculateGroupMaxUncompressedByteSize() throws IOException {
        int maxUncomp = 0;
        int i = 0;
        
        while (i < elems.size()) {
            int groupSize = getGroupSize(elems, i);

            int uncomp = 0;
            for (int j = 0; j < groupSize; j++) {
                uncomp += elems.get(i+j).getUncompressedValueByteSize();    
            }

            maxUncomp = Math.max(maxUncomp, uncomp);
            i += groupSize;
        }

        return maxUncomp;
    }

    /**
     * 
     * @param elems
     * @param currIdx
     * @return
     */
    private int getGroupSize(List<SortElem> elems, int currIdx) {
        SortElem curr = elems.get(currIdx);

        int size = 1;
        for (int i = currIdx + 1; i < elems.size(); i++) {
            if (curr.compareTo(elems.get(i)) != 0) {
                return size;
            }
            
            size++;
        }
    
        return size;
    }

    /**
     * sort element based on keybytes not key types.
     * the value coming from the temp files is compressed
     *
     */
    public class SortElem implements Comparable<SortElem> {
        public int keyOff;
        public int keyLen;
        public int valLen;
        
        // keyOff --------V
        //format:         [keyFields][valueFields]
        public SortElem(int keyOff, int keyLen, int valLen) {
            this.keyOff = keyOff;
            this.keyLen = keyLen;
            this.valLen = valLen;
        }
        
        public int getUncompressedValueByteSize() throws IOException {
            // keyOff + keyLen --> is the beginning of the compressed bytes value field
            // then we reach 3 bytes deeper to get at the postion where the varLenUnsignedInt reflecting
            // the size of the compressed bytes of this field.  BUT we want the UNCOMPRESSED size which
            // comes right after that.
            int bytesOff = keyOff + keyLen + FieldIterator.FIELD_ID_LEN + FieldIterator.FIELD_TYPE_LEN;
            
            try {
                buffer.position(bytesOff);
                VarLenNumberHelp.getVarLenUnsignedInt(buffer); // compressed
                return VarLenNumberHelp.getVarLenUnsignedInt(buffer); // uncompressed
            }
            
            catch (BufferUnderflowException e) {
                String msg = String.format("bucketNo=%d buffer.limit=%d bytesOff=%d", bucketNo, buffer.limit(), bytesOff); 
                logger.error(msg);
                throw new IOException(msg, e);
            }
        }

        public void writeKeyField(DataOutputStream stream) throws IOException {
            buffer.position(keyOff);
            for (int i = 0; i < keyLen; i++) {
                stream.write(buffer.get());
            }
        }
        
        public void writeValueBytes(DataOutputStream stream) throws IOException {
            buffer.position(keyOff + keyLen);
            for (int i = 0; i < valLen; i++) {
                stream.write(buffer.get());
            }
        }

        public void writeKeyAndValueFields(DataOutputStream stream) throws IOException {
            int len = keyLen + valLen;
            buffer.position(keyOff);
            for (int i = 0; i < len; i++) {
                stream.write(buffer.get());
            }
        }
        
        public void writeKeyFieldTo(IORecord out) {
            buffer.position(keyOff);
            for (int i = 0; i < keyLen; i++) {
                out.putByte(buffer.get());
            }
        }
        
        public void writeValueBytesTo(IOBytes out) {
            // keyOff + keyLen --> is the beginning of the value field
            int bytesOff = keyOff + keyLen + FieldIterator.FIELD_ID_LEN + FieldIterator.FIELD_TYPE_LEN;
            buffer.position(bytesOff);
            int compressedLen = VarLenNumberHelp.getVarLenUnsignedInt(buffer); // compressed
            for (int i = 0; i < compressedLen; i++) {
                out.putByte(buffer.get());
            }
        }
        
        /*
        public void writeValueFieldTo(IOBytes out) {
            // keyOff + keyLen --> is the beginning of the value field
            // here we want the value field bytes
            int bytesOff = keyOff + keyLen; // fid=2, type=1, len=4
            buffer.position(bytesOff);
            for (int i = 0; i < valLen; i++) {
                out.putByte(buffer.get());
            }
        }*/

        @Override
        public int compareTo(SortElem o2) {
            if (o2 == null) return -1;
            int lim = Math.min(keyLen, o2.keyLen);

            int i = 0;
            while (i < lim) {
                byte a = buffer.get(keyOff + i);
                byte b = buffer.get(o2.keyOff + i);
                if (a != b) {
                    return (a & 0xFF) - (b & 0xFF);
                }

                i++;
            }
            
            return keyLen - o2.keyLen;
        }
    }

    /**
     * gets the maximum file size of all the temp files.
     * 
     * @param dbDir
     * @return
     */
    private static long getMaxTempFileSize(File dbDir) {
        TempFileFilter tff = new TempFileFilter();
        File[] files = dbDir.listFiles(tff);
        long max = Long.MIN_VALUE;
        for (File temp : files) {
            max = Math.max(max, temp.length());
        }

        return max;
    }
    
    /**                                                                                                                                                                   
     * filter for the temp files of a bucket                                                                                                                                   
     */
    private static class TempFileFilter implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            String path = pathname.toString();
            return path.contains("temp");
        }
    }
}
