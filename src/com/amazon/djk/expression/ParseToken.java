package com.amazon.djk.expression;

import java.util.List;

import com.amazon.djk.expression.SlotTokenizer.ParseSlot;
import com.amazon.djk.expression.SlotTokenizer.SlotEnd;
import com.amazon.djk.expression.SlotTokenizer.SlotType;

public class ParseToken {
	private final String chunk;
	private final int tokenNo;
	private final boolean isLast;
	private final List<ParseSlot> slots;
	private final boolean isScheme;
    private final boolean endsWithTilde;
    private boolean inSubExpression = false;
	
    public ParseToken(String chunk, int tokenNo, boolean isLast) {
        this.tokenNo = tokenNo;
        this.isLast = isLast;
        
        if (chunk.endsWith("~")) {
        	endsWithTilde = true;
        	chunk = chunk.substring(0, chunk.length()-1);
        } else {
        	endsWithTilde = false;
        }
        
        this.chunk = chunk;
        
        // remove the //, leave the rest
        int pos = chunk.indexOf("://");
        if (pos != -1) {
            chunk = chunk.replace("://", ":"); 
            isScheme = true;
        } else {
            isScheme = false;
        }
        
        slots = SlotTokenizer.split(chunk);
    }
    
    public ParseToken(String chunk) {
        this(chunk, 0, true);
    }
    
    public boolean endsWithTilde() {
    	return endsWithTilde;
    }
    
    public boolean isScheme() {
        return isScheme;
    }
    
    void setInSubExpression(boolean value) {
        inSubExpression = value;
    }
    
    public boolean isInSubExpression() {
        return inSubExpression;
    }
    
	public boolean isLast() {
		return isLast;
	}
	
	public int getTokenNo() {
		return tokenNo;
	}
	
	@Override
	public String toString() {
		return chunk;
	}
	
	public String getString() {
		return chunk;
	}
	
	public List<ParseSlot> getSlots() {
	    return slots;
	}
	
	/**
	 * 
	 * @return the number of arguments contained in this token
	 */
	public int getNumArgs() {
	    // this is confusing: slots of type ARG ending in COMMA are not individual arguments in the sense
	    // of this method.  A sequence of COMMA ending ARG slots is a single FIELDS or STRINGS, etc. argument
	    int num = 0;

	    for (ParseSlot ps : slots) {
	        if ((ps.type == SlotType.ARG || ps.type == SlotType.OPTIONAL_ARG)
	             && (ps.end == SlotEnd.COLON || ps.end == SlotEnd.NONE)) {
	            num++;
	        }
	    }
	    
	    return num;
	}
	
	/**
	 * if this token has an op name, this is it.  (but if this token represents
	 * a source, then this won't be an operator name
	 * 
	 * @return return what is potentially an op name
	 */
    public String getOperator() {
        for (ParseSlot slot : slots) {
            if (slot.type == SlotType.OPNAME_POSITION) {
                return slot.string;
            }
        }
        
        return null;
    }
    
    /**
     * 
     * @return only the args and params part of the token
     */
    public String getArgsAndParams() {
		String t = getString();
		int colon = t.indexOf(':');
		if (colon != -1) {
			t = t.substring(colon+1);
		}
		
		return t;
    }

    /**
     * 
     * @return return only the params as a string
     */
    public String getParams() {
    	StringBuilder sb = new StringBuilder();
    	for (ParseSlot slot : slots) {
    		if (slot.type != SlotType.PARAM) continue;
    		if (sb.length() != 0) {
    			sb.append('&');
    		}
    		sb.append(slot.string);
    	}
    	return sb.toString();
    }
}
