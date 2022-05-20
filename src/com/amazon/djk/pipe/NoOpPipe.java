package com.amazon.djk.pipe;

import java.io.IOException;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.record.Record;

/**
 * 
 *
 */
public class NoOpPipe extends RecordPipe {

    /**
     * main constructor 
     * @param root
     * @param args
     * @throws IOException
     */
    private NoOpPipe(NoOpPipe root) throws IOException {
        super(root);
    }
	
    @Override
    public Object replicate() throws IOException {
        return new NoOpPipe(this);
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

    @Description(text={"A Null Operator. Passes records through untouched."})
    @Example(expr="[ hello:world ] noOp", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("noOp");
    	}
        	
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		return new NoOpPipe(null).addSource(operands.pop());
    	}
    }
}
