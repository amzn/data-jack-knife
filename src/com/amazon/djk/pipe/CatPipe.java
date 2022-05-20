package com.amazon.djk.pipe;

import java.io.IOException;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSource;
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
import com.amazon.djk.source.SourceListSource;

/**
 * 
 *
 */
public class CatPipe extends RecordPipe {
    
    /**
     * main constructor 
     * @param root
     * @param args
     * @throws IOException
     */
    private CatPipe(CatPipe root) throws IOException {
        super(root);
    }
	
    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;
        
        return rec;
    }
    
    @Override
    public boolean reset() {
    	return true; // nothing to do
    }

    @Description(text={"Concatenates sources together. NOTE: does not replicate across threads."})
    @Arg(name="N", gloss="number of input sources", type=ArgType.LONG)
    @Example(expr="[ hello:world ] [ hi:there ] cat:2", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("cat:N");
    	}
        	
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    	    long n = (Long)args.getArg("N");
            SourceListSource sources = new SourceListSource();
            for (int i = 0; i < n; i++) {
                sources.addSource(operands.pop());
            }

            return new CatPipe(null).addSource(sources);
    	}
    }
}
