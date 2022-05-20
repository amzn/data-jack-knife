package com.amazon.djk.record;

import java.io.IOException;
import java.util.List;

import com.amazon.djk.expression.SyntaxError;


/**
 * class for iterating over the fields that are NOT the specified fields
 */
public class NotIterator extends NamedFieldIterator {
	/**
	 * constructor to iterator over only fields of name 'field'
	 * @param field
	 * @throws IOException 
	 */
	public NotIterator(String notField) throws IOException {
		super(new String[]{notField});
	}
	
    public NotIterator(String[] notFields) throws IOException {
        super(notFields);
    }
    
    public NotIterator(List<String> notFields) throws IOException {
        super(notFields);
    }
    
    NotIterator(FieldMatcher seekFids, String[] names) throws IOException {
        super(seekFids, names);
    }
    
    public Object replicate() throws IOException  {
        return new NotIterator(matcher, names);
    }
	
	@Override
	protected boolean isFieldMatch() {
		return !matcher.isMatch(currFid);
	}
}
