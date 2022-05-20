package com.amazon.djk.record;

import java.io.IOException;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.processor.FieldDefs;

/**
 * BaseRecord class, predominately the write methods 
 *
 */
public class RecordBase extends Bytes {    
    protected static final int FIELD_ID_LEN = SHORT_SIZE;
    protected static final int FIELD_TYPE_LEN = 1;
    private static final float RECORD_DIRECT_COPY_THRESHOLD = 0.20F; // 20%
    protected int deletedBytes = 0;

    /**
     * 
     * @param field
     * @param value
     * @throws SyntaxError 
     * @throws IOException
     */
    public void addField(FieldIterator field, Record value) throws IOException {
        addField(field.getName(), value);
    }

    /**
     * 
     * @param field
     * @param value
     * @throws IOException 
     */
    public void addField(String field, Record value) throws IOException {
    	if (value.length == 0) return;
    	
        putShort(ThreadDefs.get().getOrCreateFieldId(field));
        putByte(FieldType.getFieldTypeId(FieldType.RECORD));

        // not much deleted, just copy bytes
        if ((float)value.deletedBytes / (float)value.length < RECORD_DIRECT_COPY_THRESHOLD) {
            //putInt(value.length);
            putVarLenUnsignedInt(value.length);
            putBytes(value.bytes, value.offset, value.length);
            return;
        }
        
        // else lots of wasted space, squeeze it out
        
        //TODO: use System.arraycopy to squeeze out the deleted bytes in place
        // in order to avoid the extra complete copy of the record
        Record temp = new Record();
        FieldIterator fields = new FieldIterator();
        fields.init(value);
        while (fields.next()) {
            temp.addField(fields);
        }
        putVarLenUnsignedInt(temp.length);
        putBytes(temp.bytes, temp.offset, temp.length);
    }

    /**
     * 
     * @param field
     * @param value
     * @throws IOException 
     */
    public void addField(FieldIterator field, String value) throws IOException {
        addField(field.getName(), value);
    }
    
    /**
     * 
     * @param name name of the field
     * @param value value of the field
     * @throws IOException 
     */
    public void addField(String name, String value) throws IOException {
        // field non-existence equivalent field.value == null
        if (value == null) return;
        short fid = ThreadDefs.get().getFieldIdOrSetLocal(name, value);
        if (fid != -1) {
            putShort(fid);
            putByte(FieldType.getFieldTypeId(FieldType.STRING));
            UTF8BytesRef utf8BytesRef = ThreadDefs.get().getUTF8BytesRef(value);
            putVarLenUnsignedInt(utf8BytesRef.length);
            putBytes(utf8BytesRef);
        }
    }

    /**
     * 
     * @param field
     * @param ref
     * @throws IOException 
     */
    public void addField(FieldIterator field, UTF8BytesRef ref) throws IOException {
        addField(field.getName(), ref);
    }
    
    public void addField(String name, UTF8BytesRef ref) throws IOException {
        short fid = ThreadDefs.get().getFieldIdOrSetLocal(name, ref);
        if (fid != -1) {
            putShort(fid);
            putByte(FieldType.getFieldTypeId(FieldType.STRING));
            putVarLenUnsignedInt(ref.length);
            putBytes(ref);
        }
    }
    
    /**
     * 
     * @param field
     * @param value
     * @throws IOException 
     */
    public void addField(FieldIterator field, double value) throws IOException {
        addField(field.getName(), value);
    }
    
    /**
     * 
     * @param name
     * @param value
     * @throws IOException 
     */
    public void addField(String name, double value) throws IOException {
        short fid = ThreadDefs.get().getFieldIdOrSetLocal(name, value);
        if (fid != -1) {
            putShort(fid);
            putByte(FieldType.getFieldTypeId(FieldType.DOUBLE));
            putVarLenDouble(value);
        }
    }
    
    /**
     * 
     * @param field
     * @param value
     * @throws IOException 
     */
    public void addField(FieldIterator field, long value) throws IOException {
        addField(field.getName(), value);
    }
    
    /**
     * 
     * @param name
     * @param value
     * @throws IOException 
     */
    public void addField(String name, long value) throws IOException {
        short fid = ThreadDefs.get().getFieldIdOrSetLocal(name, value);
        if (fid != -1) {
            putShort(fid);
            putByte(FieldType.getFieldTypeId(FieldType.LONG));
            putVarLenSignedLong(value);
        }
    }
    
    /**
     * 
     * @param field
     * @param value
     * @throws IOException 
     */
    public void addField(FieldIterator field, boolean value) throws IOException {
        addField(field.getName(), value);
    }
    
    /**
     * 
     * @param name
     * @param value
     * @throws IOException 
     */
    public void addField(String name, boolean value) throws IOException {
        short fid = ThreadDefs.get().getFieldIdOrSetLocal(name, value);
        if (fid != -1) {
            putShort(fid);
            putByte(FieldType.getFieldTypeId(FieldType.BOOLEAN));
            putBoolean(value);
        }
    }
    
    /**
     * 
     * @param fields
     * @throws IOException 
     */
    public void addField(FieldIterator fields) throws IOException {
        addField(null, fields);
    }
        
    /**
     * 
     * @param asName
     * @param field
     * @throws IOException 
     */
    public void addField(String asName, FieldIterator fields) throws IOException {
        if (asName != null) {
            putShort(ThreadDefs.get().getOrCreateFieldId(asName));
            putBytes(fields.bytes, fields.offset, fields.length);
        } 
        
        else {
            // include field id
            putBytes(fields.bytes, fields.offset-FIELD_ID_LEN, fields.length+FIELD_ID_LEN);         
        }
    }
    
    public void addNULL(FieldIterator field) throws IOException {
        addNULL(field.getName());
    }
    
    /**
     * adds a field with a null value
     * @param name
     * @throws IOException 
     */
    public void addNULL(String name) throws IOException {
        putShort(ThreadDefs.get().getOrCreateFieldId(name));
        putByte(FieldType.getFieldTypeId(FieldType.NULL));
    }
    
    /**
     * add raw bytes as field
     * 
     * @param field
     * @param value
     * @throws IOException
     */
    public void addField(FieldIterator field, Bytes value) throws IOException {
        addField(field.getName(), value);
    }

    /**
     * 
     * @param field
     * @param value
     * @throws IOException 
     */
    public void addField(String field, Bytes value) throws IOException {
        putShort(ThreadDefs.get().getOrCreateFieldId(field));
        putByte(FieldType.getFieldTypeId(FieldType.BYTES));
        putVarLenUnsignedInt(value.length);
        putBytes(value.bytes, value.offset, value.length);
    }
    

    /**
     * 
     * @param source the record whose fields should be added to this record
     */
    public void addFields(Record source) {
        putBytes(source);
    }
    
    /**
     * delete the currently referenced field
     * 
     * @param field
     */
    public void deleteField(FieldIterator fields) {
        deletedBytes += fields.length;
        putShortAt(fields.offset-FIELD_ID_LEN, FieldDefs.DELETED_FIELD_ID);
    }
    
    /**
     * update (delete/add) the field
     * 
     * @param 
     * @param value
     * @throws IOException 
     */
    public void updateField(FieldIterator field, long value) throws IOException {
        deleteField(field);
        addField(field, value);
    }
    
    /**
     * update (delete/add) the field
     * 
     * @param 
     * @param value
     * @throws IOException 
     */
    public void updateField(FieldIterator field, double value) throws IOException {
        deleteField(field);
        addField(field, value);
    }
    
    /**
     * update (delete/add) the field
     * 
     * @param 
     * @param value
     * @throws IOException 
     */
    public void updateField(FieldIterator field, String value) throws IOException {
        deleteField(field);
        addField(field, value);
    }
}
