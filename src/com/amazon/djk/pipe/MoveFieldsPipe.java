package com.amazon.djk.pipe;

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
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Pairs;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.Value;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

import java.io.IOException;

@ReportFormats(headerFormat="<pairs>%s")
public class MoveFieldsPipe extends RecordPipe {
    @ScalarProgress(name="pairs")
	private final Pairs pairs;
	private final OpArgs args;

    public MoveFieldsPipe(OpArgs args) throws IOException {
        this(null, args);
    }
	
    /**
     * replica constructor 
     * @param root
     * @param args
     * @throws IOException
     */
    private MoveFieldsPipe(MoveFieldsPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        pairs = (Pairs)args.getArg("PAIRS");
    }
	
    @Override
    public Object replicate() throws IOException {
        return new MoveFieldsPipe(this, args);
    }
    
    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;
        
        pairs.reset();
        while (pairs.next()) {
            Value value = pairs.value();
            String toName = value.getValueAsString(rec);

            Field fromField = pairs.field();
            String fromName = fromField.getName();
            if (fromField.isIndirect()) {
                fromField.init(rec);
                if (fromField.next()) {
                    fromName = fromField.getValueAsString();
                    if (fromName == null) continue;
                }
            }

            // mv:id:id1,size:size1 is the expected usage but these will also work:
            // mv:id:{s.fieldIndirect;} 
            // mv:id:@fieldIndirect
            // mv:id:{s.fieldIndirect + "mySuffix"} 
            
            if (fromName.isEmpty() || toName.isEmpty()) {
                continue;
            }
                        
            rec.renameAll(fromName, toName);
        }
        
        return rec;
    }
    
    @Override
    public boolean reset() {
    	return true; // nothing to do
    }

    @Description(text={"moves fields to new names."})
    @Arg(name="PAIRS", gloss="from:to pairs of fields to move", type=ArgType.PAIRS, eg="id:id1,size:size1")
    @Example(expr="[ id:1,size:12 ] mv:id:id1,size:size1", type=ExampleType.EXECUTABLE)
    @Example(expr="[ sometext:'hello world',ref:sometext ] mv:@ref:title", type=ExampleType.EXECUTABLE)
    @Example(expr="[ sometext:'hello world',ref:title ] mv:sometext:@ref", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("mv:PAIRS");
    	}
        	
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		return new MoveFieldsPipe(args).addSource(operands.pop());
    	}
    }
}
