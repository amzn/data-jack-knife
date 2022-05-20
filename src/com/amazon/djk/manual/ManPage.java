package com.amazon.djk.manual;

import com.amazon.djk.expression.Operator;

/**
 * dummy operator for providing man pages for arbitrary topics  
 */
public class ManPage extends Operator {

	public ManPage(String usageString) {
		super(usageString);
	}
}
