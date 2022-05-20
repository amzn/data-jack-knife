package com.amazon.djk.record;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.amazon.djk.format.ReaderFormatParser;
import com.amazon.djk.processor.FieldDefs;

/**
 * This is more memory efficient representation of a Record
 * for the purpose of Maps.  Eventually this implementation
 * should take over the current Record implementation and
 * be called Record
 *
 * Has the following advantages.
 * 
 * 1) supports recursive records (not just one level deep subrecords)
 * 2) no restrictions on the values stored (colons, pipes)
 * 3) Fields dictionary means only 2 bytes stored per field name.
 * 4) no fields offset integer array to resize and maintain
 * 5) fast rename can be implemented without delete and add
 * 
 */
public class Record extends RecordRead {
	
    /**
     * Value can return String,Long,Double,Boolean
     * 
     * @param field
     * @param value value to add
     * @throws IOException 
     */
    public void addField(Field field, Value value) throws IOException {
        Object oval = value.getValue(this);
        // a null value is equivalent to non-existent field
        if (oval == null) return;
        
        String name = null;
        if (field.isIndirect()) {
            field.init(this);
            name = field.getValueAsString();
            if (name == null) return;
        } else {
            name = field.getName();
        }
        
        if (oval instanceof Field) {
            addField(name, (Field)oval);
            return;
        }
        
        if (oval instanceof String) {
            addField(name, (String)oval);
            return;
        }
        
        if (oval instanceof Long) {
            addField(name, (Long)oval);
            return;
        }
        
        if (oval instanceof Double) {
            addField(name, (Double)oval);
            return;
        }
        
        if (oval instanceof Boolean) {
            addField(name, (Boolean)oval);
            return;
        }
    }

    /**
     * adds valueToBeTyped into field, where valueToBeTyped is interpretted as either double, long, boolean or String
     *
     * @param field
     * @param valueToBeTyped
     */
    public void addFieldTyped(String field, String valueToBeTyped) throws IOException {
        ReaderFormatParser.addPrimitiveValue(this, field, valueToBeTyped);
    }

    /**
     * adds the current pair of the iteration
     * 
     * @param pairs the pairs iteration to add from
     * @throws IOException 
     */
    public void addField(Pairs pairs) throws IOException {
        Value value = pairs.value();
        Field field = pairs.field();
        
        // not a function of this record
        if (value.getType() == Value.ValueType.PRIMITIVE && !field.isIndirect()) {
            ReaderFormatParser.addPrimitiveValue(this, field.getName(), value.getInputString());
        }
        
        else { // else we are a function of 'this', i.e., the record
            addField(field, value);
        }
    }
    
    /**
     * 
     * @param name name of the field
     * @throws IOException
     */
    public void deleteAll(String name) throws IOException {
        Field field = ThreadDefs.get().getCachedField(name);
        deleteAll(field);
    }

    /**
     * 
     * @param fields
     */
    public void deleteAll(FieldIterator fields) {
        fields.init(this);
        while (fields.next()) {
            putShortAt(fields.offset-FIELD_ID_LEN, FieldDefs.DELETED_FIELD_ID);
            deletedBytes += fields.length;
        }
    }

    public void renameField(FieldIterator fields, String toName) throws IOException {
        short fid = ThreadDefs.get().getOrCreateFieldId(toName);
        putShortAt(fields.offset-FIELD_ID_LEN, fid);
    }
    
    /**
     * 
     * @param fromField
     * @param toField
     * @throws IOException 
     */
    public void renameAll(String fromField, String toField) throws IOException {
        Field from = ThreadDefs.get().getCachedField(fromField);
        from.init(this);
        short toFid = ThreadDefs.get().getOrCreateFieldId(toField);       
        while (from.next()) {
            putShortAt(from.offset-FIELD_ID_LEN, toFid);
        }
    }
    
    /**
     * 
     * @param withTypes if true primitives fields annotated with type
     * @return
     * @throws IOException
     */
	public String getAsNV2(boolean withTypes) throws IOException {
		return toString(this, 0, withTypes);		
	}
	
	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	public String getAsNV2() throws IOException {
		return getAsNV2(false);
	}

	@Override
	public String toString() {
		try {
			return getAsNV2(false);
		} catch (IOException e) {
			return super.toString();
		}
	}
	/**
	 * 
	 * create an NV2 parseable representation of the record
	 * 
	 * @param rec
	 * @param level
	 * @return
	 * @throws IOException 
	 */
	private String toString(Record rec, int level, boolean withTypes) throws IOException {
		//Fields fields = FDict.local().getCachedFields(level);
		//StringBuilder sb = FDict.local().getCachedStringBuilder(level);
		FieldIterator fields = new FieldIterator();
		StringBuilder sb = new StringBuilder();

		fields.init(this);
		while (fields.next()) {
			sb.append(fields.getName());
			sb.append(':');
			if (fields.getType() == FieldType.RECORD) {
				Record subrec = new Record();
				fields.getValueAsRecord(subrec);
				String[] lines = subrec.toString(this, level + 1, withTypes).split("\\n");
				for (String line : lines) {
					sb.append('\n');
					sb.append('\t');
					sb.append(line);
				}
			}

			else {
				sb.append(fields.getValueAsString());
				if (withTypes) {
					sb.append(" [");
					sb.append(fields.getType());
					sb.append("]");
				}
			}
			
			sb.append('\n');
		}
		
		return sb.toString();
	}

	/**
	 * 
	 * @return a copy
	 * @throws IOException 
	 */
	public Record getCopy() throws IOException {
		Record copy = new Record();
		if (length == 0 || deletedBytes / length < 0.30) {
			copy.putBytes(this);
			return copy;
		}

		// eliminate deleted fields
		FieldIterator fields = ThreadDefs.get().getFieldIterator();
		fields.init(this);
		while (fields.next()) {
			copy.addField(fields);
		}
		
		return copy;
	}

    /**
     * compresses the raw bytes of this record.  
     * 
     * @return a compressed Bytes representation of the record
     * @throws IOException 
     */
    public void writeCompressedTo(Bytes out) throws IOException {
        ThreadDefs.get().deflate(this, out);
    }
    
	/**
	 * methods friendly for accessing via VALUE(djkJava) 
	 */

	/**
	 * 
	 * @param csvFields a comma separated list of field names
	 * @return true if the fields of this record are a subset of csvFields list
	 * @throws IOException 
	 */
    public boolean fieldsSubsetOf(String csvFields) throws IOException {
        // get an iterator to traverse all fields once
        OnceEachIterator recFields = new OnceEachIterator();
        Set<Short> superSetFieldIds = getFieldIdSet(csvFields);

        int numNotSubset = 0;
        recFields.init(this);;
        while (recFields.next()) {
            if (!superSetFieldIds.contains(recFields.getId())) {
                numNotSubset++;
            }
        }
        
        return numNotSubset == 0;
    }

    /**
     * 
     * @param csvFields a comma separated list of field names
     * @return true if the fields of this record are a superset of csvFields list
     * @throws IOException 
     */
    public boolean fieldsSupersetOf(String csvFields) throws IOException {
        // get an iterator to traverse all fields once
        OnceEachIterator recFields = new OnceEachIterator();
        Set<Short> superSetFieldIds = getFieldIdSet(csvFields);

        int numSetFields = 0;
        recFields.init(this);;
        while (recFields.next()) {
            if (superSetFieldIds.contains(recFields.getId())) {
                numSetFields++;
            }
        }
        
        return numSetFields == superSetFieldIds.size();
    }

    private Set<Short> getFieldIdSet(String csvFields) throws IOException {
        if (csvFields == null) return Collections.emptySet();
        String[] fields = csvFields.split(",");
        Set<Short> fieldIds = new HashSet<>(fields.length);
        for (String field : fields) {
            fieldIds.add(ThreadDefs.get().getId(field));
        }

        return fieldIds;
    }
}
