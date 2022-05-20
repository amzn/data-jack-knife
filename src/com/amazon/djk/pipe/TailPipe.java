package com.amazon.djk.pipe;

import java.io.IOException;

import com.amazon.djk.source.QueueSource;
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

/**
 * This pipe doesn't replicate so it will cause execution to be single threaded.
 * Intended for testing until someone rewrites it.  
 *
 */
@ReportFormats(headerFormat="max=%,d")
public class TailPipe extends RecordPipe {
    @ScalarProgress(name="max", aggregate=AggType.NONE)
    private final long maxRecs;
    private final QueueSource queue = new QueueSource();
    private boolean fillMode = true;
    private final OpArgs args;

    public TailPipe(OpArgs args) throws IOException {
        this(null, args);
    }
    
    public TailPipe(TailPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        maxRecs = (Long)args.getArg("MAX");
    }
    
    @Override
    public Object subReplicate() throws IOException {
    	return new TailPipe(this, args);
    }
    
    @Override
    public boolean reset() {
    	fillMode = true;
    	queue.clear();
    	return true;
    }
    
    @Override
    public Record next() throws IOException {
        if (fillMode) {
            while (true) {
                Record rec = super.next();
                if (rec == null) {
                    fillMode = false;
                    return next(); // RECURSE once.
                }
            
                if (queue.size() >= maxRecs) {
                    queue.next(); // discard.
                }
            
                queue.add(rec, true); // make copy
            }
        }
         
        return queue.next();
    }
    
    @Description(text={"tail is named after unix tail. It returns the last MAX records in both the record and sub-record contexts. In the record context it forces single threaded execution."})
    @Arg(name="MAX", gloss="maximum number of records to pass through.", type=ArgType.LONG)
    @Example(expr="[ id:last id:second id:first ] tail:1", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("tail:MAX");
    	}

    	@Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            return new TailPipe(args).addSource(operands.pop()); 
        }
    }
}
