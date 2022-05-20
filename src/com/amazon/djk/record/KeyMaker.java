package com.amazon.djk.record;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * For creating Keys for KeySource/Sink.  Fields=* not allowed.
 * The key fields are written in consistent order into the key record
 *
 */
public class KeyMaker {
    private final List<Field> keyFields;
    
    /**
     * 
     * @param fieldNames
     * @throws IOException 
     */
    public KeyMaker(String[] fieldNames) throws IOException {
        this(new Fields(fieldNames));
    }

    @Override
    public String toString() {
        return StringUtils.join(keyFields, ",");
    }

    /**
     * 
     * @param fields
     * @throws IOException 
     */
    public KeyMaker(Fields fields) throws IOException {
        if (fields.isAllFields()) {
            throw new RuntimeException("* not allowed as key fields");
        }

        this.keyFields = fields.getAsFieldList();        
    }
    
    /**
     * copies the key fields in a consistent order into the output record
     *  
     * @param from the record containing fields from which to make a key
     * @param to the output record to contain the key fields
     * @throws IOException 
     */
    public void copyTo(Record from, Record to) throws IOException {
        for (Field field : keyFields) {
            field.init(from);
        }
        
        for (Field field : keyFields) {
            if (field.next()) {
                to.addField(field);
            }
            
            else {
                // store a null value for this field
                to.addNULL(field);
            }
        }
    }
    
    /**
     * removes the key fields from the target record.
     * 
     * @param target
     */
    public void removeFrom(Record target) {
    	for (Field field : keyFields) {
    		target.deleteField(field);
    	}
    }
    
    public int getNumFields() {
        return keyFields.size();
    }
}
