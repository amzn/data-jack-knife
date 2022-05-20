package com.amazon.djk.core;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazon.djk.record.Record;
import com.amazon.djk.record.ThreadDefs;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.ProgressData;
import com.amazon.djk.report.ProgressReportFactory;

public class RecordPipe implements RecordSource {
    private final RecordPipe root;
	private RecordSource source = null; 
    protected NodeReport report = null;
	private boolean suppressReport = false;

    protected final IsLastSynchronizer isLastSync1;
    protected final IsLastSynchronizer isLastSync2;
	private final AtomicInteger numInstances;
	private final AtomicInteger numDoneInstances;
	private final int instanceNo;
	private final int numSinkThreads;
	private boolean isClosed = false;

	
	// timing fields
	private TimingType timingType;
	private long numNexts = 0;
	private long afterNanos;
	private long prevAfterNanos = 0;
	private long elapsedSourceNanos = 0;
	private long elapsedSinkNanos = 0;
	
	private enum TimingType {
		NONE,
		PIPE_SOURCE, // we are a pipe, our source is a source
		PIPE_PIPE,   // we are a pipe, our source is a pipe, 
		SINK_SOURCE, // we are a sink, our source is a source
		SINK_PIPE,   // we are a sink, our source is a pipe
	}

	/**
	 * 
	 * @param root the root instance of this pipe or null if root itself
	 * @throws IOException 
	 */
	public RecordPipe(RecordPipe root) throws IOException {
	    this.root = root;
	    numInstances = root != null ? root.numInstances : new AtomicInteger(0); 
	    numDoneInstances = root != null ? root.numDoneInstances : new AtomicInteger(0); 
	    // FIXME: bad pattern below
        isLastSync1 = root != null ? root.isLastSync1 : new IsLastSynchronizer(this); 
        isLastSync2 = root != null ? root.isLastSync2 : new IsLastSynchronizer(this); 
	    instanceNo = numInstances.getAndIncrement();
		numSinkThreads = ThreadDefs.get().getNumSinkThreads();
	}

	/**
	 * 
	 * @return the root of this predicate
	 */
	protected RecordPipe root() {
	    return root != null ? root : this;
	}
	
	@Override
	public Record next() throws IOException {
		Record rec;
		long b4;
        numNexts++;
		
		switch (timingType) {
		default:
		case NONE:
	        prevAfterNanos = 0;
			return source.next();
			
		case PIPE_SOURCE:
			b4 = System.nanoTime();			
	        rec = source.next();
			afterNanos = System.nanoTime();		
			elapsedSourceNanos += (afterNanos - b4);
			return rec;
			
		case PIPE_PIPE:
	        rec = source.next();
			afterNanos = System.nanoTime();		
			elapsedSourceNanos += (afterNanos - ((RecordPipe)source).getAfterNanos());
			return rec;
			
		case SINK_PIPE:
			b4 = System.nanoTime();
	        rec = source.next();
			afterNanos = System.nanoTime();		
			elapsedSourceNanos += (afterNanos - ((RecordPipe)source).getAfterNanos());
			elapsedSinkNanos += (prevAfterNanos != 0 ? b4 - prevAfterNanos : 0); 
			prevAfterNanos = afterNanos;
			return rec;
			
		case SINK_SOURCE:
			b4 = System.nanoTime();			
	        rec = source.next();
			afterNanos = System.nanoTime();		
			elapsedSourceNanos += (afterNanos - b4);
			elapsedSinkNanos += (prevAfterNanos != 0 ? b4 - prevAfterNanos : 0); 
			prevAfterNanos = afterNanos;
			return rec;
		}
    }
	
	/**
	 * 
	 * @return System.nanoTime() sampled immediately after next()
	 */
	public long getAfterNanos() {
		return afterNanos;
	}
	
	public double sourceRecsPerSecond() {
		return elapsedSourceNanos == 0 ? 0 : (double)1000000000.0 * (double)numNexts / (double)elapsedSourceNanos;
	}
	
	public double sinkRecsPerSecond() {
		return elapsedSinkNanos == 0 ? 0 : (double)1000000000.0 * (double)numNexts / (double)elapsedSinkNanos;
	}

	/**
	 * 
	 * @return the source of this pipe
	 */
	public RecordSource getSource() {
		return source;
	}
	
	/**
	 * TODO: change name to setSource();
	 * 
	 * @param source
	 * @return
	 */
    public RecordPipe addSource(RecordSource source) {
        this.source = source;
        if (instanceNo != 0) {
        	timingType = TimingType.NONE;
        	return this;
        } 

        if (this instanceof RecordSink) {
        	timingType = source instanceof RecordPipe ? TimingType.SINK_PIPE : TimingType.SINK_SOURCE;
        } else {
        	timingType = source instanceof RecordPipe ? TimingType.PIPE_PIPE : TimingType.PIPE_SOURCE;        		
        }

        return this;
    }
	
	@Override
	public void close() throws IOException {
        if (isClosed) return; 
        source.close();
        isClosed = true; // close only once
        numDoneInstances.incrementAndGet();
    }
	
	/*
     * If a pipe needs to prepare itself after execution (next() returns null)
     * to again be capable of executing on a newly supplied Source, it should do
     * so here and return true.
     * 
     * However, pipes are assumed to be capable of auto-reseting themselves after
     * next() returns null.  If such reset logic has not been implemented, subclasses
     * can override this method to return false which will cause execution
     * of the expression to fail with a SyntaxError.
     * 
     */
	public boolean reset() {
		return true;
	}

    /**
     * called by the framework 
     * @return true if the predicate chain has been reset
     */
	public final boolean doReset() {
		if (!reset()) return false;
		RecordSource source = getSource();
		if (source instanceof RecordPipe) {
			return ((RecordPipe)source).doReset();
		}
		return true; // source is a RecordSource (sources are replaced)
	}
	
	/**
	 * 
	 * @return the zero-based replica number
	 */
	public int getInstanceNo() {
		return instanceNo;
	}
	
	public int getNumInstances() {
		return numInstances.get();
	}
	
	public int getNumActiveInstances() {
		return Math.min(numSinkThreads, getNumUnfinishedInstances());
	}
	
	/**
	 * 
	 * @return the number of running instances (including the root)
	 */
	public int getNumUnfinishedInstances() {
		return numInstances.get() - numDoneInstances.get();
	}
	
	public final Object getStrand() throws IOException {
		return getStrand(false);
	}
	
	 /**
     * 
     * @return a sink representing an individual execution graph to be threaded or
     * null when there are no more strands and this Sink should be used as the last
     * strand to be threaded.
     */
	public final Object getStrand(boolean isSubExpression) throws IOException {
	    // if we don't replicate we're done
        if (!isReplicable(isSubExpression)) {
                return null;
        }

        // we only remove strands from Sinks/Pipes 
        // for sources if they implement Splittable, split off a portion
        Object strand = (source instanceof RecordPipe) ?
                        ((RecordPipe)source).getStrand(isSubExpression) :
                        (source instanceof Splittable) ?
                        ((Splittable)source).split() : null;            
        
        // if strand is null then the source has no strands, return null
        if (strand == null) return null; 
        
        // in the case of splitted source, add the splitted report to its source
        if (strand instanceof Splittable) {
            NodeReport report = ((RecordSource)source).getReport();
            report.addProvider((RecordSource)strand);
        }
        
        // at this point we have a strand and we know we can replicate so do it.
        
        RecordPipe replica = (RecordPipe)replicate(isSubExpression);
        if (replica == null) {
                // this is a coding error.
                throw new RuntimeException(this.getClass().getSimpleName() + ".replicate() and .subReplicate() methods should never return null.");
        }
        
        NodeReport report = getReport(); // get our report
        report.addProvider(replica); // add the replica as a provider of report data
        
        // we can replicate, return end strand in a replica of 'this'
        replica.addSource((RecordSource)strand);
            
        return replica;
    }
	
	/**
     * 
     * @param isSubExpression is the context of this pipe a subExpression
     * @return true if this pipe replicates.
     */
	private boolean isReplicable(boolean isSubExpression) {
	    return isSubExpression ? isSubReplicable() : isReplicable();
    }
    
    /**
     * reflect whether this class replicates or not.
     * @return true if this class replicates
     */
    private boolean isReplicable() {
        try {
            Method meth = this.getClass().getDeclaredMethod("replicate", (Class<?>[])null);
            return meth != null;
        } catch (NoSuchMethodException | SecurityException e) {
            return false;
        }
    }
    
    /**
     * reflect whether this class subReplicates or not.  Since the base class defines
     * subReplicate as replicate, in the event subReplicate is not implemented, if
     * 'replicate' is implemented, return true;
     * @return true if this class subReplicates
     */
    private boolean isSubReplicable() {
        try {
            Method meth = this.getClass().getDeclaredMethod("subReplicate", (Class<?>[])null);
            return meth != null;
        } catch (NoSuchMethodException | SecurityException e) {
            return isReplicable();
        }
    }

	
	/**
	 * Pipes optionally implement replicate which is used in the
	 * mainExpression context.  If replicate() returns non-null then
	 * this pipe is capable of running in a multi threaded/stranded
	 * context, otherwise only single stranded/threaded.
	 * 
	 * An override of this method must never return null!
	 */
	public Object replicate() throws IOException {
		return null;
	}
	
    public Object replicate(boolean isSubExpression) throws IOException {
        return isSubExpression ? subReplicate() : replicate(); 
    }
    
    /**
     * All Pipes MUST implement subReplicate.  This should be equivalent
     * to the item created by constructor of the Pipe.  Ideally we should
     * rename replicate and subReplicate.
     * 
     * subReplicate --> replicate
     * creates an object identical to the getAsPipe object and does not
     * coordinate across threads for use at the subExpression level.
     * 
     * replicate --> coordinatedReplicate
     * creates an object that coordinates with the getAsPipe object
     * for use in the mainExpression level
	 *
     * 
     * @return
     * @throws IOException
     */
	public Object subReplicate() throws IOException {
		return replicate();
	}

	@Override
	public ProgressData getProgressData() {
		return new ProgressData(this);
	}

    @Override
    public final void suppressReport(boolean value) {
        suppressReport = value;
    }
    
    @Override
    public boolean isReportSuppressed() {
    	return suppressReport;
    }

    /**
     * @return the hierarchical report 
     */
    public NodeReport getReport() {
        return getReport(null);
    }
    
    /**
     * 
     * @return the hierarchical report 
     */
    public NodeReport getReport(String nodeName) {
    	if (report != null) return report; // singleton object
    	report = ProgressReportFactory.create(this, nodeName);

    	if (source != null) {
        	// is refering to an extending class ok?
    		if (this instanceof RecordSink) {
        	    report.addChildReport( source.getReport() );
    		} else {
        	    report.addLeftReport( source.getReport() );
    		}
    	}
    	
    	return report;
    }
}
