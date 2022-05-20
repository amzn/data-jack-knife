package com.amazon.djk.expression;

import java.util.List;

import com.amazon.djk.expression.SlotTokenizer.ParseSlot;
import com.amazon.djk.expression.SlotTokenizer.SlotType;

/**
 * verify legality, parse usage into components, create a regex for matching.
 * 
 * allow ARG1=predicate:ARG2:ARG3
 * allow AFIELDS=predicate:BFIELDS
 * allow usage=foo:PAIRS to match foo:goo:boo,zoo:loo --> PAIRS matches foo:goo,zoo:loo
 * arg names must be uniq
 * only one colon position allowed on left side of = (and only of type FIELD or FIELDS)
 * PAIRS only allowed on rhs and as the last arg
 * 
 */
public class OpUsage {
	private final String usageString;
	private final String opName;
	private final ParseToken usageToken;
	   
	public OpUsage(String usageString) {
		this.usageString = usageString;
		usageToken = new ParseToken(usageString);

        String name = null;
        for (int i = 0; i < usageToken.getSlots().size(); i++) {
            ParseSlot slot = usageToken.getSlots().get(i);
            if (slot.type == SlotType.OPNAME_POSITION) {
                if (i > 1) {
                    throw new ProgrammingException("illegal to define more than one ARG position left of the equals (only FIELD or FIELDS allowed)");    
                }
                name = slot.string;
            }
        }
        
        this.opName = name;
	}
	
	public String getOpName() {
	    return opName;
	}

	/**
	 * 
	 * @return the argument names of the operator in order
	 */
	public List<ParseSlot> getSlots() {
	    return usageToken.getSlots();
	}
	
	@Override
	public String toString() {
	    return usageString;
	}
}
