package com.amazon.djk.record;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.amazon.djk.expression.SyntaxError;

/**
 * class for iterating over the data positions of a record.  Positions hold either a
 * field or sub-record. 
 */
public class NamedFieldIterator extends FieldIterator {
    protected final String[] names;
	
	/**
	 * 
	 * 
	 * @param fields
	 * @throws IOException 
	 */
    public NamedFieldIterator(String[] fields) throws IOException {
        this(Arrays.asList(fields));
	}
    
    public NamedFieldIterator(String field) throws IOException {
        this(Arrays.asList(new String[]{field}));
    }
    
    /**
     * constructor for traversing all fields
     * @throws IOException 
     */
    public NamedFieldIterator() {
        super(new HashSet<Short>());
        names = new String[0];
    }
	
    /**
     * main constructor
     * @param fields
     * @throws IOException 
     */
    public NamedFieldIterator(List<String> fields) throws IOException {
        super(getSeekFids(fields));
        this.names = dedupeFields(fields);
    }

    public NamedFieldIterator(Set<Short> seekFids, String[] fields) {
        super(seekFids);
        this.names = fields;
    }
    
    public NamedFieldIterator(FieldMatcher seekFids, String[] fields) {
        super(seekFids);
        this.names = fields;
    }
    
    private static String[] dedupeFields(List<String> fields) throws SyntaxError {
        if (fields == null) throw new IllegalFieldException("null fields invalid");
        return fields.stream().distinct().toArray(String[]::new);
    }
    
    protected static Set<Short> getSeekFids(List<String> infields) throws IOException  {
        String[] fields = dedupeFields(infields);

        Set<Short> seekFids = new HashSet<>();
        for (String field : fields) {
            seekFids.add(ThreadDefs.get().getOrCreateFieldId(field));
        }

        return seekFids;
    }

	public Object replicate() throws IOException  {
	    return new NamedFieldIterator(matcher, names);
	}
	
	public NotIterator getAsNotIterator() throws IOException {
	    return new NotIterator(matcher, names);
	}
	
	/**
	 * 
	 * @return
	 */
	public String[] getFieldNames() {
	    return names;
	}
	
	/**
	 * 
	 * @return the number of fields or Integer.MAX_VALUE if a wild card FIELDS
	 */
	public int numFields() {
	    return names.length;
	}
	
	@Override
    public String toString() {
        return StringUtils.join(names, ",");
    }
}
