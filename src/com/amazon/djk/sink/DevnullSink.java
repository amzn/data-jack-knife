package com.amazon.djk.sink;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.record.Record;

/**
 * 
 * Sink that discards records. 
 *
 */
public class DevnullSink extends RecordSink {
    public final static String NAME = "devnull";
	
    public DevnullSink() throws IOException {
        this(null);
    }
    
    public DevnullSink(RecordSink root) throws IOException {
        super(root);
    }

    @Override
    public void drain(AtomicBoolean forceDone) throws IOException {
    	super.drain(forceDone);
    	
        while (!forceDone.get()) {
            Record rec = next();
            if (rec == null) {
            	break;
            }
            
            reportSunkRecord(1);
        }
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new DevnullSink(this);
    }
    
    @Description(text={"the record bit bucket."})
    @Example(expr="djk blanks:10 devnull", type=ExampleType.DISPLAY_ONLY)
    public static class Op extends PipeOperator {
    	public Op() {
    		super(NAME);
    	}
    
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		return new DevnullSink().addSource(operands.pop());
    	}
    }
}
