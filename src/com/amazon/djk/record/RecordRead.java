package com.amazon.djk.record;

import java.io.IOException;

/**
 * predominately the Record read methods 
 *
 */
public class RecordRead extends RecordBase {

    /**
     * 
     * @param name name of the field
     * @return the first instance of this field as a Long or null if non-existant or impossible
     * @throws IOException 
     */
    public Long getFirstAsLong(String name) throws IOException {
        Field field = ThreadDefs.get().getCachedField(name);
        return getFirstAsLong(field);
    }
    
    /**
     * 
     * @param field
     * @return
     * @throws IOException 
     */
    public Long getFirstAsLong(Field field) throws IOException {
        Object o = field.initOrGetLocal(this);
        if (o != null) return (Long)o;
        if (!field.next()) return null;
        return field.getValueAsLong();
    }
    
    public Boolean getFirstAsBoolean(String name) throws IOException {
        Field field = ThreadDefs.get().getCachedField(name);
        return getFirstAsBoolean(field);
    }
    
    /**
     * 
     * @param field
     * @return
     * @throws IOException 
     */
    public Boolean getFirstAsBoolean(Field field) throws IOException {
        Object o = field.initOrGetLocal(this);
        if (o != null) return (Boolean)o;
        if (!field.next()) return null;
        return field.getValueAsBoolean();
    }

    /**
     * 
     * @param name
     * @return
     * @throws IOException 
     */
    public Double getFirstAsDouble(String name) throws IOException {
        Field field = ThreadDefs.get().getCachedField(name);     
        return getFirstAsDouble(field);
    }
    
    /**
     * 
     * @param field name of the field
     * @return the first instance of this field as a Long or null if non-existent or impossible
     * @throws IOException 
     */
    public Double getFirstAsDouble(Field field) throws IOException {
        Object o = field.initOrGetLocal(this);
        if (o != null) return (Double)o;
        if (!field.next()) return null;
        return field.getValueAsDouble();
    }
    
    /**
     * 
     * @param name
     * @return
     * @throws IOException 
     */
    public String getFirstAsString(String name) throws IOException {
        Field field = ThreadDefs.get().getCachedField(name);
        return getFirstAsString(field);
    }
    
    /**
     * 
     * @param field name of the field
     * @return the first instance of this field as a Long or null if non-existant or impossible
     * @throws IOException 
     */
    public String getFirstAsString(Field field) throws IOException {
        Object o = field.initOrGetLocal(this);
        if (o != null) return (String)o;
        if (!field.next()) return null;
        return field.getValueAsString();
    }
    
    /**
     * 
     * @param name
     * @param outrec
     * @throws IOException 
     */
    public boolean getFirstAsRecord(String name, Record outrec) throws IOException {
        Field field = ThreadDefs.get().getCachedField(name);
        return getFirstAsRecord(field, outrec);
    }
    
    /**
     * 
     * @param field
     * @param outrec
     */
    public boolean getFirstAsRecord(Field field, Record outrec) {
        field.init(this);
        outrec.reset();
        if (!field.next()) return false;
        field.getValueAsRecord(outrec);
        return true;
    }

    /**
     * 
     * @param name
     * @return
     * @throws IOException 
     */
    public Record getFirstAsRecord(String name) throws IOException {
        Field field = ThreadDefs.get().getCachedField(name);
        return getFirstAsRecord(field);
    }
    
    /**
     * 
     * @param field
     * @return
     * @throws IOException 
     */
    public Record getFirstAsRecord(Field field) throws IOException {
        field.init(this);
        if (!field.next()) return null;
        Record rec = new Record();
        field.getValueAsRecord(rec);
        return rec;
    }
    
    /**
     * A UTF8 BytesRef is a bytes ref where the offset points to the beginning of the utf8
     * and the length is the number of bytes.  (In contrast to a FieldBytesRef).
     * 
     * @param field name of the field
     * @return the first instance of this field as a Long or null if non-existant or impossible
     */
    public boolean getFirstAsUTF8BytesRef(Field field, UTF8BytesRef ref) {
        field.init(this);
        ref.bytes = null;
        ref.offset = 0;
        ref.length = 0;
                
        if (!field.next()) return false;
        return field.getValueAsUTF8BytesRef(ref);
    }
    
    /**
     * A UTF8 BytesRef is a bytes ref where the offset points to the beginning of the utf8
     * and the length is the number of bytes.
     * 
     * @param name
     * @param ref
     * @throws IOException 
     */
    public boolean getFirstAsUTF8BytesRef(String name, UTF8BytesRef ref) throws IOException {
        Field field = ThreadDefs.get().getCachedField(name);
        return getFirstAsUTF8BytesRef(field, ref);
    }

    /**
     * 
     * @param name
     * @return
     * @throws IOException 
     */
    public UTF8BytesRef getFirstAsUTF8BytesRef(String name) throws IOException {
        UTF8BytesRef ref = new UTF8BytesRef();
        if (getFirstAsUTF8BytesRef(name, ref)) {
            return ref;
        }

        return null;
    }
    
    /**
     * 
     * @param name
     * @param ref
     * @return
     * @throws IOException 
     */
    public boolean getFirstAsFieldBytesRef(String name, FieldBytesRef ref) throws IOException {
        Field field = ThreadDefs.get().getCachedField(name);
        field.init(this);
        if (!field.next()) return false;
        field.getValueAsFieldBytesRef(ref);
        return true;
    }
}
