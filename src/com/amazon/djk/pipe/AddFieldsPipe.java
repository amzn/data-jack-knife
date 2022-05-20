package com.amazon.djk.pipe;

import java.io.IOException;

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
import com.amazon.djk.record.Pairs;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

@ReportFormats(headerFormat="<pairs>%s")
public class AddFieldsPipe extends RecordPipe {
    @ScalarProgress(name="pairs")
	private final Pairs pairs;
	private final OpArgs args;

    public AddFieldsPipe(OpArgs args) throws IOException {
        this(null, args);
    }
	
    /**
     * replica constructor 
     * @param root
     * @param args
     * @throws IOException
     */
    private AddFieldsPipe(AddFieldsPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        pairs = (Pairs)args.getArg("PAIRS");
    }
	
    @Override
    public Object replicate() throws IOException {
        return new AddFieldsPipe(this, args);
    }
    
    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;
        
        pairs.reset();
        while (pairs.next()) {
            rec.addField(pairs);
        }
        
        return rec;
    }
    
    @Override
    public boolean reset() {
    	return true; // nothing to do
    }

    @Description(text={"adds fields to the incoming records. If the VALUE of a PAIR results in null, the field will not be added to the record.  See VALUE."})
    @Arg(name="PAIRS", gloss="name value pairs of fields to add", type=ArgType.PAIRS)
    @Example(expr="[ id:1,size:12 ] add:status:ok,price:3.0,euSize:'{l.size * 4;}'", type=ExampleType.EXECUTABLE)
    @Example(expr="blanks:1 add:useJavaSyntaxForStringsWithSpaceOrPunct:'{\"[ hello : there [ \";}'", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("add:PAIRS");
    	}
        	
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		return new AddFieldsPipe(args).addSource(operands.pop());
    	}
    }
}
