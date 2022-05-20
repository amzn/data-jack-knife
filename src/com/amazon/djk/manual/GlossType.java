package com.amazon.djk.manual;

public enum GlossType {
	/**
	 * glosses pertaining to the syntax of an expression containing this predicate
	 * i.e. the parameters of the predicate or the other elements of the expression
	 */
	SYNTACTIC,
	
	/**
	 * of the form name=value appended to the uri for a source/sink using the conventions
	 * of URI, e.g. sink:/local/disk?name1=value1&name2=value2
	 */
	URI_PARAM,
	
	/**
	 * glosses describing the fields that input records to this predicate operate on 
	 * e.g. txtnorm requires either 
	 *   
	 */
	RECORD_FIELD
}
