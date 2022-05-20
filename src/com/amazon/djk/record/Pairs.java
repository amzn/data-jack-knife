package com.amazon.djk.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazon.djk.expression.SyntaxError;

/**
 * Operator argument helper class.  For operators that need pairs of input.
 * The first element of the pair is a Field, The second is a Value.
 * The class can also be used to retrieve the pairs as Map<String,String> for
 * generic pair usage.
 */
public class Pairs {
	private final String pairsExpression;
	private final List<Field> fields;
	private final List<Value> values = new ArrayList<>();
	private final Map<String,String> map = new HashMap<>();
	int idx = -1;

	/**
	 * external constructor 
	 * @param names
	 * @param values
	 * @throws IOException
	 * @throws SyntaxError
	 */
	public Pairs(List<Field> fields, List<Value> values) throws IOException, SyntaxError {
	    this(fields, values, isValid(fields.size() == values.size()));
	}
	
	private static boolean isValid(boolean isValid) throws SyntaxError {
        if (!isValid) {
            throw new SyntaxError("improper Pairs construction");
        }

        return isValid;
	}
	
	/**
	 * internal constructor throws no syntax error
	 * 
	 * @param names
	 * @param values
	 * @param internal
	 * @throws IOException
	 * @throws SyntaxError
	 */
	public Pairs(List<Field> fields, List<Value> values, boolean isValid) throws IOException {
	    this.fields = fields;
	    for (Value value : values) {
	        this.values.add((Value)value.replicate());
	    }
	    
	    StringBuilder sb = new StringBuilder();
	    for (int i = 0; i < fields.size(); i++) {
	        if (i != 0) sb.append(',');
	        String name = fields.get(i).getName();
	        String value = values.get(i).toString();
	        
	        sb.append(name);
	        sb.append(':');
	        sb.append(value);
	        
	        map.put(name, value);
	    }
	    
	    pairsExpression = sb.toString();
	}
	
	/**
	 * 
	 * @return a Map<String,String> of the pairs (instead of the underlying Field, Value)
	 */
	public Map<String,String> getAsMap() {
		return map;
	}
	
	@Override 
	public String toString() {
		return pairsExpression;
	}

	public Object replicate() throws IOException {
	    return new Pairs(fields, values, true);
	}
	
	/**
	 * reset the iterator.
	 */
	public void reset() {
		idx = -1;
	}

	public boolean next() {
		return ++idx < fields.size();
	}
	
	/**
	 * 
	 * 
	 */
	public Field field() {
		return fields.get(idx);
	}
	
	/**
	 * 
	 * @return the current UN-INITIALIZED value
	 */
	public Value value() {
		return values.get(idx);
	}
}
