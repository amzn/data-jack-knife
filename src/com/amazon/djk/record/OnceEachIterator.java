package com.amazon.djk.record;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.record.FieldMatcher.OnceMatcher;

/**
 * class for iterating over the fields and matching only once for each
 * distinct field.
 */
public class OnceEachIterator extends NamedFieldIterator {
    /**
     * constructor to iterate over all fields once
     * 
     * @throws IOException
     */
    public OnceEachIterator() throws IOException {
        this(new String[0]);
    }
	
    /**
     * constructor to iterate over just 'fields' once
     * @param fields
     * @throws IOException 
     */
    public OnceEachIterator(String[] fields) throws IOException {
        this(Arrays.asList(fields));
    }

    /**
     * constructor to iterate over just 'fields' once
     * @param fields
     * @throws IOException 
     */
    public OnceEachIterator(List<String> fields) throws IOException {
        super(getOnceMatcher(fields), new String[0]);
    }
    
    private static OnceMatcher getOnceMatcher(List<String> fields) throws IOException {
        return new OnceMatcher(getSeekFids(fields));
    }
    
    @Override
    public final void init(RecordBase rec) {
        ((OnceMatcher)matcher).reset();
        super.init(rec);
    }
    
    @Override
    public Object replicate() throws IOException {
        try { // we've already constructed so shouldn't throw
            return new OnceEachIterator(names);
        } catch (SyntaxError e) {
            return null;
        }
    }
}
