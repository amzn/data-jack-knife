package com.amazon.djk.reducer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.pipe.IfPipe;
import com.amazon.djk.expression.CommaList;

/**
 * Holds a map of Reducer instance name to RecordSource representing its reduction
 * for a given expression (i.e. RecordSink and its graph)
 *
 */
public class Reductions {
	private final Map<String, LazyReductionSource> reductions = new HashMap<>(); 
	
	public Reductions(RecordSink sink) throws IOException {
		assignReductions(sink, reductions);
	}

	/**
	 * create a graph with the source being the reducers with the given instance names.
	 * after this call the reductions associated with 'instances' are removed from
	 * the inner map.
	 * 
	 * @param instances the instance names of the reducers to be sunk via reduceChunks
	 * @param reduceChunks
	 * @param parser
	 * @param mainSink
	 * @return
	 * @throws IOException 
	 */
	public LazyReductionSource getAssignedReduction(CommaList instances) throws IOException {

		// iname -> reduction
		//Map<String, RecordSource> reductions = getReductions(mainSink);
		
		String[] instanceArray = instances.array();
		if (instanceArray.length == 1) {
			return reductions.remove(instanceArray[0]);
		}

		// bring together to reduction instances mapped to a particular reduction expression
		LazyReductionSource together = null;
		for (String instance : instanceArray) {
			LazyReductionSource next = reductions.remove(instance);
			if (next == null) { // means no reducer has such an instance
				throw new SyntaxError("No reducer instance has instance="+instance);
			}

			if (together == null) {
				together = next;
			} else {
				together.add(next);				
			}
		}
		
		return together;
	}
	
	/**
	 * 
	 * @param parser
	 * @return
	 */
	public LazyReductionSource getUnassignedReduction() {
		if (reductions.size() == 0) return null; // everything is gone

		Collection<LazyReductionSource> unassignedSources = reductions.values();
		LazyReductionSource together = null;
		for (LazyReductionSource next : unassignedSources) {
			if (together == null) {
				together = next;
			} else {
				together.add(next);				
			}
		}
		
		return together;
	}
	
	/**
	 * 
	 * @param expression
	 * @param reductions
	 * @throws IOException
	 */
	private static void assignReductions(RecordPipe expression, Map<String,LazyReductionSource> reductions) throws IOException {
		RecordSource source = expression;
		
		while (true) {
			if (source instanceof Reducer) {
				Reducer reducer = (Reducer)source;
				if (reducer.getInstanceNo() != 0) {
					throw new IOException("must call using mainSink");
				}
				
				String instance = reducer.getInstanceName();
				
				// see if there is already a reduction with
				// this iname, if so, bundle them together
				LazyReductionSource current = reductions.get(instance);
				if (current != null) {
					current.add(reducer.getLazyCrossStrandReduction());
				} else {
					// get the RecordSource that collects the reduction from all threads
					reductions.put(instance, reducer.getLazyCrossStrandReduction());
				}
			}
			
			if (source instanceof IfPipe){
				assignReductions(((IfPipe)source).getTrueClause(), reductions);
			    RecordPipe falseClause = ((IfPipe)source).getFalseClause(); 
			    if(falseClause != null){
			    	assignReductions(falseClause, reductions);
			    }
			}

			if (source instanceof RecordPipe) {
				source = ((RecordPipe) source).getSource();
				continue;
			}

			// else strict RecordSource, we're done
			return;
		}
	}
	
	/**
	 * 
	 * 
	 * @param childExpr
	 * @param reducers
	 */
	public static void collectChildReducers(RecordPipe childExpression, List<Reducer> reducers) {
		 RecordSource source = childExpression;
		                
		 while (true) {
			 if (source instanceof Reducer) {
				 Reducer reducer = (Reducer)source;
				 reducers.add(reducer);
			 }
		                        
			 if (source instanceof IfPipe){
				 collectChildReducers(((IfPipe)source).getTrueClause(), reducers);
				 RecordPipe falseClause = ((IfPipe)source).getFalseClause(); 
				 if(falseClause != null){
					 collectChildReducers(falseClause, reducers);
				 }
			 }
			 
			 if (source instanceof RecordPipe) {
				 source = ((RecordPipe) source).getSource();
				 continue;
			 }
			 
			 // else strict RecordSource, we're done
			 return;
		 }
	}
}
