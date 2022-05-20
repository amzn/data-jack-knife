package com.amazon.djk.record;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.processor.FieldDefs;

/**
 * class to do fast translation of the field ids written to disk and the current
 * field ids as held by the global field dictionary 
 *
 */
public class RecordIO {
    private final static int FIELD_TYPE_LEN = Record.FIELD_TYPE_LEN;
    public enum Direction {STORED_TO_LIVE, LIVE_TO_STORED};
    private final short[] fidsStoredToLive;
    private final short[] fidsLiveToStored;
    private final String[] storedFieldNames;
    
    /**
     * Record subclass to expose Byte() methods 
     *
     */
    public static class IORecord extends Record {
        private int numResizes = 0;
        
        public void putByte(byte b) {
            super.putByte(b);
        }
        
        public void set(ByteBuffer map, int mapPosition, int numBytes) {
            reset();
            super.resize(numBytes);
            map.position(mapPosition);
            map.get(bytes, 0, numBytes);
            length = numBytes;
        }

        public int numResizes() {
            return numResizes;
        }
        
        public void resize(int size) {
        	byte[] b4bytes = bytes;
            super.resize(size);
            if (b4bytes != bytes) {
            	numResizes++;
            }
        }
        
        public void putBytes(byte[] bytes, int offset, int length) {
            super.putBytes(bytes, offset, length);
        }

        public void setLength(int len) {
            this.length = len;
        }
        
        public void resetButKeepCapacity() {
            length = 0;
        }
        
        public void addBytesAsRecordField(Field field, IOBytes bytes, boolean decompressFirst) throws IOException {
            putShort(ThreadDefs.get().getOrCreateFieldId(field.getName()));
            putByte(FieldType.getFieldTypeId(FieldType.RECORD));

            if (!decompressFirst) {
                //putInt(bytes.size());
                putVarLenUnsignedInt(bytes.size());
                putBytes(bytes);
                return;
            }
            
            Bytes temp = ThreadDefs.get().getCachedBytes();
            ThreadDefs.get().inflate(bytes, temp);
            putVarLenUnsignedInt(temp.size());
            putBytes(temp);
        }
    }

    /**
     * 
     */
    public static class IOBytes extends Bytes {
        public void putByte(byte b) {
            super.putByte(b);
        }
        
        public void putBytes(byte[] bytes, int off, int add) {
            super.putBytes(bytes, off, add);
        }
        
        public void putShort(short value) {
            super.putShort(value);
        }
        
        public void putInt(int value) {
            super.putInt(value);
        }
        
        public void resize(int add) {
            super.resize(add);
        }
        
        public void setLength(int len) {
            this.length = len;
        }
        
        public boolean read(DataInputStream stream, int len) throws IOException {
            resize(len);
            int toRead = len;
            while (toRead != 0) {
                int numRead = stream.read(bytes, length, toRead);
                if (numRead == -1) {
                    return false;
                }  

                length += numRead;
                if (numRead == toRead) break;
                toRead -= numRead;
            }
            
            return true;
        }

        public void resetButKeepCapacity() {
            length = 0;
        }
    }
    
	public RecordIO(String[] storedFieldNames) throws IOException {
	    this.storedFieldNames = storedFieldNames;
		fidsStoredToLive = new short[storedFieldNames.length];

		// create fast translation table
		ThreadDefs defs = ThreadDefs.get();
		short maxLiveFid = -1;
		for (int storedFid = 0; storedFid < storedFieldNames.length; storedFid++) {
		    String storedName = storedFieldNames[storedFid];
		    short liveFid = defs.getOrCreateFieldId(storedName);
		    if (liveFid == FieldDefs.LOCAL_FIELD_TYPE_ID) {
		    	throw new SyntaxError(String.format("Attempt made to define '%s' as local but it already has a field id associated with it.  This is currently not allowed.", storedName));
		    }
		    fidsStoredToLive[storedFid] = liveFid;
		    maxLiveFid = (short)Math.max(maxLiveFid, liveFid);
		}
		
		fidsLiveToStored = new short[maxLiveFid + 1];
		for (int i = 0; i < fidsLiveToStored.length; i++) {
		    // fields will be deleted if they do not exist in live
		    fidsLiveToStored[i] = FieldDefs.DELETED_FIELD_ID; 
		}
		
		for (short i = 0; i < fidsStoredToLive.length; i++) {
		    short liveFid = fidsStoredToLive[i];
		    fidsLiveToStored[liveFid] = i;
		}
	}
	
	/**
	 * translates the field ids written to disk to the currently active mappings
	 * 
	 * @param keyMakerMadeRecord record created by KeyMaker for this keyed source.  (Restricts the possible fields to key fields)
	 * @throws IOException 
	 */
    public void translate(Record keyMakerMadeRecord, Direction dir) throws IOException {
        int inputEndPos = keyMakerMadeRecord.length;
        int length = 0;
        int offset = keyMakerMadeRecord.offset;
        
        short[] fidLookup = dir == Direction.STORED_TO_LIVE ? 
                fidsStoredToLive : fidsLiveToStored;
        
        do {
            offset += length;
            if (offset >= inputEndPos) break;
        
            short fid = keyMakerMadeRecord.getShortAt(offset);
            offset += Record.FIELD_ID_LEN;
            byte typeId = keyMakerMadeRecord.getByteAt(offset);
            
            switch (typeId) {
            case FieldType.BYTES_ID:
            case FieldType.STRING_ID:
                //length = Record.VAR_FIELD_PAYLOAD_PREFIX + rec.getIntAt(offset + Record.FIELD_TYPE_LEN);
                length = FIELD_TYPE_LEN + keyMakerMadeRecord.getVarLenUnsignedIntAt(offset + FIELD_TYPE_LEN) + keyMakerMadeRecord.lastNumVarLenBytes;
                break;
                
            case FieldType.RECORD_ID:
                // length set to var field payload prefix causes recursion into the subrecord
                //length = Record.VAR_FIELD_PAYLOAD_PREFIX;
                keyMakerMadeRecord.getVarLenUnsignedIntAt(offset + FIELD_TYPE_LEN);
                length = FIELD_TYPE_LEN + keyMakerMadeRecord.lastNumVarLenBytes;
                break;

            case FieldType.DOUBLE_ID:
            case FieldType.LONG_ID:
                //length = Record.FIELD_TYPE_LEN + Record.LONG_SIZE;
                keyMakerMadeRecord.getVarLenSignedLongAt(offset + FIELD_TYPE_LEN);
                length = FIELD_TYPE_LEN + keyMakerMadeRecord.lastNumVarLenBytes;
                break;
                
            case FieldType.BOOLEAN_ID:
                length = Record.FIELD_TYPE_LEN + 1; 
                break;
            
            case FieldType.NULL_ID:                
                length = Record.FIELD_TYPE_LEN; 
                break;
                
            case FieldType.ERROR_ID:
            default:
                throw new IOException("field type not implemented for conversion:" + typeId);
            }

            if (fid >= fidLookup.length) {
                throw new IOException(String.format("The field-id of 'keyMakerMadeRecord' argument outside of bounds of %s translation array.  It likely was not created the appropriate or any KeyMaker.", dir));
            }
            
            short newFid = fidLookup[fid];
            if (fid != newFid) {
                keyMakerMadeRecord.putShortAt(offset-Record.FIELD_ID_LEN, newFid);
            }
            
        } while (true);
    }
    
    /**
     * 
     * @param recordBytes raw record bytes 
     * @param out the translated record using the constructor field names.
     * @throws IOException 
     */
    public void translate(byte[] recordBytes, Record out, Direction dir) throws IOException {
        out.reset();
        out.putBytes(recordBytes, 0, recordBytes.length);
        translate(out, dir);
    }
	
    public boolean fill(DataInputStream stream, RecordFIFO out) throws IOException {
        out.reset();
        return fill(stream, out.storage);
    }
    
	/**
	 * 
	 * @param stream
	 * @param fifo
	 * @return
	 * @throws IOException
	 */
    public boolean fill(DataInputStream stream, IORecord out) throws IOException {
	    int reclen = 0;
	    out.reset();

	    try {
	        reclen = stream.readInt();
	        out.resize(reclen);
	        int offset = 0;
	        int toRead = reclen - offset;
	        while (toRead != 0) {
	            int numRead = stream.read(out.bytes, offset, toRead);
	            if (numRead == -1) {
	                return false;
	            }
			
	            if (numRead == toRead) break;
	            offset += numRead;
	            toRead -= numRead;
	        }
	    }
	
	    catch (EOFException e) {
	        return false;
	    }
	    
		out.length = reclen;
		translate(out, Direction.STORED_TO_LIVE);
		return true;
	}
	
    public static void write(DataOutputStream outstream, RecordFIFO fifo) throws IOException {
        write(outstream, fifo.storage);
    }
    
	/**
	 * prepends each record of the stream with its length
	 * 
	 * @param outstream
	 * @throws IOException
	 */
    public static void write(DataOutputStream outstream, Record record) throws IOException {
		outstream.writeInt(record.length);
		outstream.write(record.bytes, 0, record.length);
	}
}
