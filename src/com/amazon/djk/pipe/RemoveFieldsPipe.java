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
public class RemoveFieldsPipe extends RecordPipe {
    private static final String INPUTS = "INPUTS";
    @ScalarProgress(name="fields")
	final Fields fields;
	private final OpArgs args;
	private final FieldIterator fiter;

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public RemoveFieldsPipe(OpArgs args) throws IOException {
	    this(null, args);
	}

	/**
	 * 
	 * @param root
	 * @param args
	 * @throws IOException
	 */
    public RemoveFieldsPipe(RemoveFieldsPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        fields = (Fields)args.getArg(INPUTS);
        fiter = fields.getAsIterator();
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new RemoveFieldsPipe(this, args);
    }

    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;

        fiter.init(rec);
        while (fiter.next()) {
            rec.deleteField(fiter);
        }

        return rec;
    }
    
   @Description(text={"Removes fields from the incoming records."})
   @Arg(name=INPUTS, gloss="the fields to remove", type=ArgType.FIELDS)
   @Example(expr="[ id:1,color:blue ] rm:color", type=ExampleType.EXECUTABLE)
   public static class Op extends PipeOperator {
       public Op() {
           super("rm:INPUTS");
       }
           
       @Override
       public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
           return new RemoveFieldsPipe(args).addSource(operands.pop());
       }
   }
}
