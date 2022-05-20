package com.amazon.djk.reducer;

import java.io.IOException;

import com.amazon.djk.record.Record;

/**
 * a record pipe that passes through records from input to output
 * but that also performs reduction on those records.  There are two
 * contexts that such pipes must operate in, record and sub-record level
 * 
 * 1) at the mainExpression level, the reductions are collected by the djk
 * framework and can be processed at the end of the main execution
 * by the --reduceout <expression> switch.  E.g. the reductions could
 * be sent to an nvp file. 
 * 
 * 2) at the subExpression level, the reductions (generally one record)
 * are effectively joined into the parent whose sub-records are  
 * being processed in the sub-expression
 * 
 * Implementations need to be able to deal with handle both contexts
 *  
 */
public interface IReducer {
	/**
	 * This method is called by the SubExpression Pipe (after exhausting next() 
	 * of this reducer), and the fields of this record are added to the parent
	 * record.  
	 * @return
	 * @throws IOException
	 */
	Record getChildReduction() throws IOException;
	
	/**
	 * This method is called by the core for each strand after exhausting the strand. 
	 * If the getAggregator method returns non-null, these records will be passed through
	 * for further reduction. 
	 * 
	 * Base functionality is provided by Reducer.  Extending classes can optionally override 
     * this method if their application offers different logic for parent or child reduction.
     * Care must be taken to lazily evaluate next() since reduction expressions are parsed before
     * execution.
	 * 
	 * @return
	 * @throws IOException
	 */
	Record getNextMainReduction() throws IOException;
	
	/**
	 * Subclasses that require reduction across threads should provide an aggregator
	 * capable of further reducing the records returned by getCrossStrandAggregator().  This
	 * is not always the case since some reducers share common resources (e.g an
	 * AtomicLong counter) whereby getNextMainReduction() on any instance must yield
	 * the reduction records.  In this case getCrossStrandAggregator() should return null;
	 * 
	 * @return
	 * @throws IOException
	 */
	ReducerAggregator getCrossStrandAggregator() throws IOException;

	/**
	 * The ReducerOperator provides an instance name to a reducer.  The default name is 'main'.
	 * This makes it possible to send the reduction to different sinks via the REDUCE keyword. 
	 * 
	 * djk source OP OP reducer1?instance=base OP OP reducer1?instance?modified OP OP
	 * OP OP reducer2?instance=vanilla OP OP reduce3 devnull
	 * 
	 *  REDUCE:base,modified OP OP nat:reduce1.out
	 *  REDUCE:vanilla OP OP nat:reduce2.out
	 *  REDUCE:main OP OP reduce3.out
	 * 
	 * @return the name of this instance of this type of reducer
	 */
	String getInstanceName();
}
