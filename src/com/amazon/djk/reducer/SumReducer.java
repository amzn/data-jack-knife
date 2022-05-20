package com.amazon.djk.reducer;

import java.io.IOException;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.Value;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;

/**
 *
 *
 */
@ReportFormats2(headerFormat="<outfield>%s += <input>%s",
lineFormats={"numSums=%d nonNums=%d"})
public class SumReducer extends Reducer {
    private final OpArgs args;
    @ScalarProgress(name="input")
    private final Value input;
    @ScalarProgress(name="outfield")
    private final Field outfield;

	private double result = 0.0;

	@ScalarProgress(name="nonNums")
	private long numNonEvals = 0;
	
	@ScalarProgress(name="numSums")
	private long numSums = 0;
	
	private final boolean asLong;
	private final Record outrec = new Record();

	public SumReducer(SumReducer root, OpArgs args) throws IOException {
	    super(root, args, Type.BOTH);
	    this.args = args;
	    input = (Value)args.getArg("INPUT");
	    asLong = (Boolean)args.getParam("asLong");
        outfield = (Field)args.getArg("OUTPUT");
	}
	
	// we do not multi-thread in mainExpression context so no replicate
	
	@Override
	public Object subReplicate() throws IOException {
		return new SumReducer(this, args);
	}
	
	@Override
	public Record getChildReduction() throws IOException {
		outrec.reset();
		if (asLong) {
			outrec.addField(outfield, Math.round(result));
		} else {
			outrec.addField(outfield, result);
		}
		
		return outrec;
	}
	
	@Override
	public boolean reset() {
		result = 0.0;		
		return true;
	}

	@Override
	public Record next() throws IOException {
		Record rec = super.next();
		if (rec == null) return null;

		try {
            Object obj = input.getValue(rec);
            if (obj == null) {
        		numNonEvals++;
            }
            
            // in this case, see if the string is a field name, this will allow
            // the syntax of total=sum:weight instead of requiring total=sum:'{l.weight;}'
            // for the most common use case. 
            else if (obj instanceof String) {
            	result += rec.getFirstAsDouble((String)obj);
            }
            
            else if (obj instanceof Long) {
            	Long l = (Long)obj;
            	result += (double) l;
            }
            		
            else if (obj instanceof Double) {
            	result += (double) obj;
            }
		
            else { // non-Long/Double
            	numNonEvals++;
            }
		}
		
		// allow for non-existant fields to return 0.0
        catch (NullPointerException e) {
            numNonEvals++;
        }
		
		numSums++;
		return rec;
	}
	
	/**
	 * 
	 *
	 */
	
	@Description(text={"Sums INPUT into OUTPUT."})
    @Arg(name="INPUT", gloss="the VALUE expression which must evaulate to long or double (see VALUE)", type=ArgType.VALUE)
	@Arg(name="OUTPUT", gloss="output field of the reduced record", type=ArgType.FIELD)
	@Param(name="asLong", gloss="round all values to Long.", type=ArgType.BOOLEAN, defaultValue="false")
	@Example(expr="[ ferry:Orcas,car:[weight:15],car:[weight:40],car:[weight:4] ] [ total=sum:weight'?asLong=true' foreach:car ", type=ExampleType.EXECUTABLE)
	@Example(expr="[ ferry:Orcas,car:[weight:15,color:blue],car:[weight:40,color:blue],car:[weight:4,color:green] ] [ blueTotal=sum:'{s.color.equals(\"blue\") ? l.weight : 0;}' foreach:car ", type=ExampleType.EXECUTABLE)	
    public static class Op extends ReducerOperator {
		public Op() {
			super("OUTPUT=sum:INPUT");
		}
	
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            return new SumReducer(null, args).addSource(operands.pop());
        }
	}
}

