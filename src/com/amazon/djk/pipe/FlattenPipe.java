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
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

import java.io.IOException;


@ReportFormats(headerFormat="<fields>%s")
public class FlattenPipe extends RecordPipe {
    private static final String INPUTS = "SUBS";
    @ScalarProgress(name="fields")
	final Fields fields;
	private final OpArgs args;
	private final FieldIterator fiter;
	private final Record sub = new Record();
	
	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public FlattenPipe(OpArgs args) throws IOException {
	    this(null, args);
	}

	/**
	 * 
	 * @param root
	 * @param args
	 * @throws IOException
	 */
    public FlattenPipe(FlattenPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        fields = (Fields)args.getArg(INPUTS);
        fiter = fields.getAsIterator();
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new FlattenPipe(this, args);
    }

    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;

        fiter.init(rec);
        while (fiter.next()) {
        	if (fiter.getValueAsRecord(sub, true)) {
        		rec.addFields(sub);
        	}
        	
            rec.deleteField(fiter);
        }

        return rec;
    }
    
   @Description(text={"Flattens sub-records into the parent. Fields are effectively moved from the sub-record to the parent,",
		   "so if there are multiple instances of a sub-record, there will be multiple instances of the flattened fields.",
		   "If a SUB represents a field instead of a sub-record, it will be removed.  See also 'denorm'."})
   @Arg(name=INPUTS, gloss="the sub-records to flatten into the parent", type=ArgType.FIELDS)
   @Example(expr="[ color:blue,sub:[id:1,size:big] ] flatten:sub", type=ExampleType.EXECUTABLE)
   public static class Op extends PipeOperator {
       public Op() {
           super("flatten:SUBS");
       }
           
       @Override
       public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
           return new FlattenPipe(args).addSource(operands.pop());
       }
   }
}
