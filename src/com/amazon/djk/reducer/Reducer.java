package com.amazon.djk.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

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
@ReportFormats(lineFormats={"<instance>%s"}) // we don't want to show this for child reductions
public abstract class Reducer extends RecordPipe implements IReducer {
	static final ThreadLocal<AtomicInteger> instanceIds =
			new ThreadLocal<AtomicInteger>() {

		@Override protected AtomicInteger initialValue() {
			return new AtomicInteger(1);
		}
	};

	public enum Type {BOTH, MAIN_ONLY, CHILD_ONLY};
	private final List<Reducer> threadSiblings;
	@ScalarProgress(name="instance")
	private final String instanceDisplay; // if null not shown
	private final String instance;
	private boolean isMainIterationDone = false;
	
	public Reducer(Reducer root, OpArgs args, Type reducerType) throws IOException {
		this(root, getInstanceName(args), reducerType);
	}

	/**
	 * 
	 * @param root
	 * @param instanceName
	 * @param reducerType
	 * @throws IOException
	 */
	public Reducer(Reducer root, String instanceName, Type reducerType) throws IOException {
        super(root);
        if (root != null) {
        	threadSiblings = root.threadSiblings;
        	root.threadSiblings.add(this); // dicey?
        	instance = root.instance;
        } else { // we're the root
        	threadSiblings = new ArrayList<>();
        	this.instance = instanceName;
        }
        
        instanceDisplay = instance == null ? null : String.format("instance=%s", instance);
    }
	
	private static String getInstanceName(OpArgs args) throws IOException {
        if (args.isInSubExpression()) return null; // mute in subexpressions 
		
        // args will have param=instance (via ReducerOperator) if present.
        String paramInstance = (String)args.getParam(ReducerOperator.INSTANCE_PARAM);

        if (paramInstance != null) {
        	return paramInstance;
        }

        // if not specified, make up a unique id
        AtomicInteger id = instanceIds.get();
        return String.format("i%d", id.getAndIncrement());
	}

    @Override
	public ReducerAggregator getCrossStrandAggregator() throws IOException {
		return null;
	}
    
    @Override
    final public String getInstanceName() {
    	return instance;
    }

    /**
     * Creates a LAZY record source which aggregates the reduction across all threads.
     * Needs to be LAZY since reduction expressions are parsed at ExecutionContext time
     * in order for the reports to available before any execution.
     * 
     * @return
     * @throws IOException
     */
    LazyReductionSource getLazyCrossStrandReduction() throws IOException {
    	ReducerAggregator aggregator = getCrossStrandAggregator();
    	
    	// if no siblings or if no aggregator
    	if (threadSiblings.isEmpty() || aggregator == null) {
    		return new LazyReductionSource(this);
    	}
    	
    	LazyReductionSource sss = new LazyReductionSource(this);
    	for (Reducer reducer : threadSiblings) {
    		sss.add(reducer);
    	}

    	aggregator.addSource(sss);
    	
    	return new LazyReductionSource(aggregator, instance);
    }
    
    /**
     * default is to return one of the child reduction
     */
    @Override
    public Record getNextMainReduction() throws IOException {
    	if (isMainIterationDone) return null;
    	isMainIterationDone = true;
    	return getChildReduction();
    }
}
