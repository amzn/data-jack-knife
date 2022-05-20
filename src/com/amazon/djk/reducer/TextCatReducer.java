package com.amazon.djk.reducer;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;

import java.io.IOException;

/**
 * concatenates the text of FIELDS together into the OUT field of the reduced record  
 *
 */
@ReportFormats2(headerFormat="<outfield>%s+=<infields>%s")
public class TextCatReducer extends Reducer {
    private final OpArgs args;
    private final FieldIterator fiter;
    @ScalarProgress(name="infields")
    private final Fields infields;
    @ScalarProgress(name="outfield")
    private final Field outfield;

	private final StringBuilder result = new StringBuilder();
	private final Record outrec = new Record();

	public TextCatReducer(TextCatReducer root, OpArgs args) throws IOException {
	    super(root, args, Type.BOTH);
	    this.args = args;
	    infields = (Fields)args.getArg("INPUTS");
        fiter = infields.getAsIterator();
        outfield = (Field)args.getArg("OUTPUT");
	}
	
	// we do not multi-thread in mainExpression context so no replicate
	
	@Override
	public Object subReplicate() throws IOException {
		return new TextCatReducer(this, args);
	}
	
	@Override
	public Record getChildReduction() throws IOException {
		outrec.reset();
		outrec.addField(outfield, result.toString());
		return outrec;
	}
	
	@Override
	public boolean reset() {
		result.setLength(0);
		return true;
	}

	@Override
	public Record next() throws IOException {
		Record rec = super.next();
		if (rec == null) return null;

		fiter.init(rec);
		while (fiter.next()) {
		    if (result.length() > 0) {
	            result.append(' ');
		    }
		    result.append(fiter.getValueAsString());
		}
		
		return rec;
	}
	
	/**
	 * 
	 *
	 */
	
	@Description(text={"Concatenates INPUTS together with a single space into OUTPUT."})
    @Arg(name="INPUTS", gloss="fields to concatenate together", type=ArgType.FIELDS)
	@Arg(name="OUTPUT", gloss="output field of the reduced record", type=ArgType.FIELD)
	@Example(expr="[ id:1,phrase:[text:'one fish'],phrase:[text:'two fish'] ] [ alltext=txtcat:text foreach:phrase ", type=ExampleType.EXECUTABLE)
    public static class Op extends ReducerOperator {
		public Op() {
			super("OUTPUT=txtcat:INPUTS");
		}
	
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            return new TextCatReducer(null, args).addSource(operands.pop());
        }
	}
}

