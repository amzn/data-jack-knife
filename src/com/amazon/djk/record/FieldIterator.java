package com.amazon.djk.record;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.amazon.djk.processor.FieldDefs;
import com.amazon.djk.record.FieldMatcher.OneFieldMatcher;
import com.amazon.djk.record.FieldMatcher.UnmatchableFieldMatcher;

/**
 * class for iterating over the data positions of a record.  Positions hold either a
 * field or sub-record. 
 */
public class FieldIterator extends BytesRef {
    public static final int FIELD_ID_LEN = Record.SHORT_SIZE;
    public static final int FIELD_TYPE_LEN = Record.FIELD_TYPE_LEN;
    byte typeId = FieldType.getFieldTypeId(FieldType.ERROR);    
    protected int inputEndPos;
    short currFid = Short.MIN_VALUE; // undefined
    long currLongOrDoubleAsLong;

    private final UTF8BytesRef utf8Ref = new UTF8BytesRef();
    protected final FieldMatcher matcher;
    private String doubleFormat = null;
    
    /**
     * main constructor
     * @param fields
     * @throws IOException 
     */
    public FieldIterator() {
        this(new HashSet<>());
    }
    
    public FieldIterator(Set<Short> seekFids) {
    	this.matcher = FieldMatcher.create(seekFids);
    }
    
    protected FieldIterator(FieldMatcher matcher) {
    	this.matcher = matcher;
    }
    
    public Object replicate() throws IOException {
        return new FieldIterator(matcher);
    }
    
    /**
     * 
     * @param rec
     */
    public void init(RecordBase rec) {
        this.bytes = rec.bytes;
        this.offset = rec.offset;
        this.inputEndPos = rec.offset + rec.length;
        currFid = Short.MIN_VALUE; // undefined
        this.length = 0; // --> 0 because of first line in next()
    }
    
    /**
     * after next() returns true:
     * 
     *        |<-------length ------>
     * offset-v
     * |fid(2)|type(1)|             |
     * 
     * @return true if a field has been found.
     */
    public boolean next() {
        if (matcher instanceof UnmatchableFieldMatcher) {
            return false;
        }
            
        do {
            offset += length;
            if (offset >= inputEndPos) return false;
        
            currFid = getShortAt(offset);
            offset += FIELD_ID_LEN;
            typeId = getByteAt(offset);
            
            switch (typeId) {
            case FieldType.STRING_ID:
            case FieldType.RECORD_ID:
            case FieldType.BYTES_ID:
                length = FIELD_TYPE_LEN + getVarLenUnsignedIntAt(offset + FIELD_TYPE_LEN) + lastNumVarLenBytes;
                break;

            case FieldType.DOUBLE_ID:
            case FieldType.LONG_ID:
                currLongOrDoubleAsLong = getVarLenSignedLongAt(offset + FIELD_TYPE_LEN);
                length = FIELD_TYPE_LEN + lastNumVarLenBytes;
                break;
                
            case FieldType.BOOLEAN_ID:
            	lastNumVarLenBytes = 0;
                length = FIELD_TYPE_LEN + 1; 
                break;
            
            case FieldType.NULL_ID:
            	lastNumVarLenBytes = 0;
                length = FIELD_TYPE_LEN; // no data 
                break;
                
            case FieldType.ERROR_ID:
            default:
            	lastNumVarLenBytes = 0;
                return false;
            }
        } while (currFid == FieldDefs.DELETED_FIELD_ID || !isFieldMatch());
        
        return true;
    }
    
    protected boolean isFieldMatch() {
        return matcher.isMatch(currFid);
    }
    
    /**
     * 
     * @return the type of the current field 
     * (undefined behavior if next() not called or returns false)
     */
    public FieldType getType() {
        return FieldType.getType(typeId);
    }

    /**
     * 
     * @return the name of the current field
     * (undefined behavior if next() not called or returns false)
     * @throws IOException 
     */
    public String getName() throws IOException {
        return ThreadDefs.get().getName(currFid); 
    }

    /**
     * 
     * @return the id of the current field
     */
    public short getId() {
        if (matcher instanceof OneFieldMatcher) {
            return ((OneFieldMatcher)matcher).getFid();
        }
        
        return currFid;
    }

    public Double getValueAsDouble() {
        switch (typeId) {
        case FieldType.LONG_ID:
            return (double)currLongOrDoubleAsLong;
            
        case FieldType.DOUBLE_ID:
            return Double.longBitsToDouble(currLongOrDoubleAsLong);

        case FieldType.STRING_ID:            
        case FieldType.BOOLEAN_ID:
        case FieldType.RECORD_ID:            
        case FieldType.NULL_ID: 
        case FieldType.BYTES_ID:
        case FieldType.ERROR_ID:
        default:
            return null;
        }
    }
    
    public Long getValueAsLong() {
        switch (typeId) {
        case FieldType.LONG_ID:
            return currLongOrDoubleAsLong;
            
        case FieldType.DOUBLE_ID:
            return (long)Double.longBitsToDouble(currLongOrDoubleAsLong);
            
         case FieldType.BOOLEAN_ID:
         case FieldType.STRING_ID:           
         case FieldType.RECORD_ID:
         case FieldType.NULL_ID:
         case FieldType.BYTES_ID:
         case FieldType.ERROR_ID:
        default:
            return null;
        }
    }

    /**
     * 
     * @param outrec
     * @return
     */
    public boolean getValueAsRecord(Record outrec) {
        return getValueAsRecord(outrec, true);
    }
    
    /**
     * 
     * @param rec
     */
    public boolean getValueAsRecord(Record outrec, boolean resetOutrec) {
        if (resetOutrec) {
            outrec.reset();
        }
        
        switch (typeId) {
        case FieldType.RECORD_ID:
            outrec.putBytes(bytes, offset + FIELD_TYPE_LEN + lastNumVarLenBytes, length - lastNumVarLenBytes - FIELD_TYPE_LEN);
            return true;
            
        case FieldType.NULL_ID:
        case FieldType.BOOLEAN_ID:
        case FieldType.STRING_ID:
        case FieldType.LONG_ID:
        case FieldType.DOUBLE_ID:
            /*
             * What was I thinking here?!?  This is too hard to reason about.
             *
                a dummy field name would be approapriate here, but for simplicity use the actual 
            int off = offset - 3;
            int len = length + 3;
            rec.putBytes(bytes, off, len);
            */
                    
        case FieldType.BYTES_ID:
        case FieldType.ERROR_ID:
        default:
              return false;
        }
    }
    
    /**
     * 
     * @return
     * @throws IOException
     */
    public String getValueAsString() throws IOException {
        switch (typeId) {

        case FieldType.RECORD_ID:
            Record rec = new Record(); 
            getValueAsRecord(rec);
            return rec.toString();
            
        case FieldType.STRING_ID:
            getValueAsUTF8BytesRef(utf8Ref);
            return utf8Ref.getAsString();
            
        case FieldType.LONG_ID:
            return Long.toString(currLongOrDoubleAsLong);
            
        case FieldType.DOUBLE_ID:
            return getDoubleAsString(Double.longBitsToDouble(currLongOrDoubleAsLong)); 
            
        case FieldType.BOOLEAN_ID:
            return Boolean.toString(getBooleanAt(offset+FIELD_TYPE_LEN));           
            
        case FieldType.NULL_ID:
        case FieldType.BYTES_ID:
        case FieldType.ERROR_ID:
        default:
            return null;
        }
    }
    
    private String getDoubleAsString(double value) throws IOException {
    	if (doubleFormat == null) {
    		doubleFormat = "%1." + ThreadDefs.get().getDoublePrintPrecision() + "f";
    	}
    	
        return String.format(doubleFormat, value);
    }

    /**
     * 
     * @param utf8Ref
     * @return true if the current field is of type STRING
     */
    public boolean getValueAsUTF8BytesRef(UTF8BytesRef utf8Ref) {
        switch (typeId) {
        case FieldType.STRING_ID:
            utf8Ref.bytes = bytes;
            utf8Ref.offset = offset + FIELD_TYPE_LEN + lastNumVarLenBytes;
            utf8Ref.length = length - FIELD_TYPE_LEN - lastNumVarLenBytes;
            return true;
            
        case FieldType.BOOLEAN_ID:
        case FieldType.LONG_ID:
        case FieldType.DOUBLE_ID:
        case FieldType.RECORD_ID:            
        case FieldType.ERROR_ID:
        case FieldType.BYTES_ID:
        case FieldType.NULL_ID:
        default:
            return false;
        }
    }
    
    /**
     * 
     * @return
     */
    public Boolean getValueAsBoolean() {
        switch (typeId) {
        case FieldType.BOOLEAN_ID:
            return getBooleanAt(offset+FIELD_TYPE_LEN);

        case FieldType.STRING_ID:
        case FieldType.LONG_ID:
        case FieldType.DOUBLE_ID:
        case FieldType.RECORD_ID:            
        case FieldType.ERROR_ID:
        case FieldType.BYTES_ID:
        case FieldType.NULL_ID:
        default:
            return null;
        }
    }
    
    /**
     * After this call, the FieldBytesRef will reference the typeId
     * followed by the raw bytes of the current field.
     * i.e. <i>without</i> the fieldId prefix.
     * 
     * @param ref to be set
     */
    public void getValueAsFieldBytesRef(FieldBytesRef ref) {
        ref.bytes = bytes;
        ref.offset = offset;
        ref.length = length;
    }
    
    /**
     * 
     * @return the FieldBytes of the current value.
     * i.e. a short of typeId followed by the field data payload
     */
    public Bytes getValueAsFieldBytes() {
        Bytes out = new Bytes();
        out.putBytes(bytes, offset, length);
        return out;
    }
    
    /**
     * After this call, the BytesRef will reference the  
     * the current field value (including any bytes encoding variable length)
     * but <i>without</i> the fieldId prefix and <i>without</i> typeId.
     * 
     * @param ref to be set
     */
    public void getValueAsBytesRef(BytesRef ref) {
        ref.bytes = bytes;
        ref.offset = offset + FIELD_TYPE_LEN;
        ref.length = length - FIELD_TYPE_LEN;
    }
    
    /**
     * 
     * @return the Bytes of the current value. (see getValueAsBytesRef)
     * 
     */
    public void getValueAsBytes(Bytes out) {
    	out.putBytes(bytes, offset + FIELD_TYPE_LEN, length - FIELD_TYPE_LEN);    	
    }
    
    /**
     * TODO: This method is wrong but broken in a way that is symmetrical to how the natdb
     * bucket sorter is also broken.  This method can be eliminated (and the above method
     * used), when natdb is fixed.
     * 
     * See also the HashPipe.  HashPipe makes it clear that if this broken method
     * is used (which formerly had the name of properly functioning method above) 
     * to get the bytes of a Long/Double field, zero bytes are returned!
     * 
     * The fix may be backward incompatible with existing natdbs (nat sources should be ok)
     *  
     * @param out
     */
    public void getValueAsBytesBROKENnatdb(Bytes out) {
    	out.putBytes(bytes, offset + FIELD_TYPE_LEN + lastNumVarLenBytes, length - FIELD_TYPE_LEN - lastNumVarLenBytes);
    }
}
