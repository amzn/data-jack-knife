package com.amazon.djk.manual;

public enum ExampleType {
	/**
	 * for examples that are only displayed as expression snippets
	 */
	DISPLAY_ONLY,
	
	/**
	 * for examples that are displayed and executed
	 * 
	 * the expr() string may only begin with the following source predicates
	 * 
	 * devinf:N where N < 10
	 * devnull
	 * rec:FIELD_PAIRS
	 *   
	 */
	EXECUTABLE,
	
	/**
	 * with graph
	 */
	EXECUTABLE_GRAPHED
}
