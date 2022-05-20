package com.amazon.djk.sink;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Record;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 * 
 */
public class PrintSink extends RecordSink {
	public final static String NAME = "print";
	private final PrintStream stream;
	private final boolean withTypes;
	FieldIterator fiter = new FieldIterator();
	
	public PrintSink(boolean withTypes) throws IOException {
		this(System.out, withTypes);
	}
	
	public PrintSink(PrintStream stream) throws IOException {
		this(stream, false);
	}
	
	private PrintSink(PrintStream stream, boolean withTypes) throws IOException {
	    super(null);
		this.stream = stream;
		this.withTypes = withTypes;
	}
	
    @Override
    public void drain(AtomicBoolean forceDone) throws IOException {
    	super.drain(forceDone);
    	
        while (!forceDone.get()) {
            Record rec = next();
            if (rec == null) break;

            stream.print(rec.getAsNV2(withTypes));
            stream.println("#"); // end of record marker
            stream.flush();
            reportSunkRecord(1);
        }
        
        stream.flush();
    }
    
    public void close() throws IOException {
        super.close();
        this.stream.flush();
    }
    
    @Description(text={"The stdout sink. At the command line, if no sink is provided, the 'print' sink is implied."})
    @Param(name="withTypes", gloss="prints field type information after the value (non-parsable).", type=ArgType.BOOLEAN, defaultValue="false")
    @Example(expr="djk [ hello:world] print", type=ExampleType.DISPLAY_ONLY)
    @Example(expr="djk [ hello:world]", type=ExampleType.DISPLAY_ONLY)
    public static class Op extends PipeOperator {
    	public Op() {
    		super(NAME);
    	}
    
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		boolean withTypes = (boolean)args.getParam("withTypes");
    		return new PrintSink(withTypes).addSource(operands.pop());
    	}
    }
}
