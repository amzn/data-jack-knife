package com.amazon.djk.pipe;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;

@ReportFormats(headerFormat="max=%,d")
public class HeadPipe extends RecordPipe {
	public static final String MAX = "MAX";
	@ScalarProgress(name="max", aggregate=AggType.NONE)
	private final long maxRecs;
	private final AtomicLong numLeft;
	
	/**
	 * for main contexts
	 * 
	 * @param num
	 * @throws IOException 
	 */
	public HeadPipe(HeadPipe root, long max) throws IOException {
        this(root, max, new AtomicLong(max));
	}
	
	/**
	 * for replicates
	 * 
	 * @param args
	 * @param numLeft
	 * @throws IOException 
	 */
	private HeadPipe(HeadPipe root, long max, AtomicLong numLeft) throws IOException {
	    super(root);
	    maxRecs = max;
        this.numLeft = numLeft;
	}

	@Override
	public Object replicate() throws IOException {
		return new HeadPipe(this, maxRecs, numLeft); // all threads decrement same counter
	}
	
	@Override
	public Object subReplicate() throws IOException {
		return new HeadPipe(this, maxRecs); // different counters
	}
	
	@Override
	public Record next() throws IOException {
	    // get record first which we may throw away, need to know if one exists
        Record rec = super.next();
        if (rec == null) return null;
	    
		long val = numLeft.decrementAndGet();
		if (maxRecs != -1 && val < 0) return null;
		
		return rec;
	}
	
	@Override
	public boolean reset() {
		numLeft.set(maxRecs);
		return true;
	}

    @Description(text={"head is named after unix head. In a sub-record context it works as expected.  In the record context it behaves like a 'grabAny MAX' predicate."})
    @Arg(name= MAX, gloss="maximum number of records to pass through. -1 means unlimited.", type=ArgType.LONG, eg="10")
    @Example(expr="[ id:1 id:2 id:3 ] head:2", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("head:MAX");
    	}

    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    	    long max = (Long)args.getArg(MAX);
    		return new HeadPipe(null, max).addSource(operands.pop()); 
    	}
    }
}
