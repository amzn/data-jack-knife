package com.amazon.djk.record;

import java.io.IOException;

import com.amazon.djk.expression.SyntaxError;

public class Field extends NamedFieldIterator {
	private final String fieldSpec;
	private final String name;
	private final boolean isIndirect; // means fieldName holds the fieldName
	
	/**
	 * constructor to iterator over only fields of name 'field'
	 * @param fieldSpec
	 * @throws IOException 
	 */
    public Field(String fieldSpec) throws IOException {
		super(new String[]{getName(fieldSpec)});
		if (fieldSpec == null) throw new IllegalFieldException("null field invalid");
		this.fieldSpec = fieldSpec;
		this.name = getName(fieldSpec);
		isIndirect = fieldSpec.charAt(0) == '@';
	}
	
    /**
     * constructor for traversing all fields
     */
    public Field() throws IOException {
        fieldSpec = "*";
        name = "";
        isIndirect = false;
    }

    /**
     * 
     * @param rec
     * @return the local value if there is one or null (always init iterator)
     * @throws IOException 
     */
    public Object initOrGetLocal(RecordBase rec) throws IOException {
        super.init(rec);
        return ThreadDefs.get().getLocalValue(name);
    }
    
    private Field(FieldMatcher seekFids, String fieldSpec) throws IOException {
    	super(seekFids, null);
        this.fieldSpec = fieldSpec;
        this.name = getName(fieldSpec);
        isIndirect = fieldSpec.charAt(0) == '@';
    }
	
	/**
	 * removes the optional 'at' symbol
	 * @param name
	 * @return
	 */
	private static String getName(String name) {
	    return name.charAt(0) == '@' ? name.substring(1) : name;
	}
	
	/**
	 *
	 * @return true if the field name is the field containing the name (i.e. @name syntax);
	 */
	public boolean isIndirect() {
	    return isIndirect;
	}
	
	@Override
	public Object replicate() throws IOException {
	    return new Field(matcher, fieldSpec);
    }
	
	@Override
	public String toString() {
	    return fieldSpec;
	}
	
	@Override
	public String getName() {
		// we want this method to always be valid to call
		return name;
	}
}
