package com.amazon.djk.reducer;

import java.io.IOException;

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
import com.amazon.djk.record.Record;
import com.amazon.djk.record.Value;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;

/**
 *
 *
 */
@ReportFormats2(headerFormat="<outfield>%s <booleanOp>%c= <input>%s",
lineFormats={"booleans=%d nonBooleans=%d"})
public class BooleanReducer extends Reducer {
    private final OpArgs args;
    @ScalarProgress(name="input")
    private final Value input;
    @ScalarProgress(name="outfield")
    private final Field outfield;

    @ScalarProgress(name="booleanOp")
    private final char booleanOp;
    
	private boolean result;

	@ScalarProgress(name="nonBooleans")
	private long numNonEvals = 0;
	
	@ScalarProgress(name="booleans")
	private long numBooleans = 0;
	private final Record outrec = new Record();

	public BooleanReducer(BooleanReducer root, char booleanOp, OpArgs args) throws IOException {
	    super(root, args, Type.BOTH);
	    this.args = args;
	    this.booleanOp = booleanOp;
	    input = (Value)args.getArg("INPUT");
        outfield = (Field)args.getArg("OUTPUT");
        result = booleanOp == '&'; // initialize based on boolean operator
	}
	
	@Override
	public NodeReport getReport() {
        return getReport(booleanOp == '&' ? "And" : "Or");
    }
	
	// we do not multi-thread in mainExpression context so no replicate
	
	@Override
	public Object subReplicate() throws IOException {
		return new BooleanReducer(this, booleanOp, args);
	}
	
	@Override
	public Record getChildReduction() throws IOException {
		outrec.reset();
		outrec.addField(outfield, result);
		return outrec;
	}
	
	@Override
	public boolean reset() {
		result = booleanOp == '&';
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
            // the syntax of existsTrue=or:myBoolean instead of requiring existsTrue=or:'{b.myBoolean;}'
            // for the most common use case. 
            else if (obj instanceof String) {
            	result = booleanOp == '|' ? 
            			result | rec.getFirstAsBoolean((String)obj) : 
            			result & rec.getFirstAsBoolean((String)obj); 
            }
            
            else if (obj instanceof Boolean) {
            	result = booleanOp == '|' ? 
            			result | (Boolean)obj : 
            			result & (Boolean)obj;
            }
		
            else { // non-Boolean
            	numNonEvals++;
            }
		}
		
		// allow for non-existant fields to return false
        catch (NullPointerException e) {
            numNonEvals++;
        }
		
		numBooleans++;
		return rec;
	}
	
	/**
	 * 
	 *
	 */
	@Description(text={"Or's INPUT into OUTPUT. Mainly used to propagate information from child records into the parent."})
    @Arg(name="INPUT", gloss="the VALUE expression which must evaulate to boolean (see VALUE)", type=ArgType.VALUE)
	@Arg(name="OUTPUT", gloss="output field of the reduced record", type=ArgType.FIELD)
	@Example(expr="[ ferry:Orcas,car:[weight:15],car:[weight:40],car:[weight:4] ] [ tooHeavyCarExists=or:'{l.weight > 20L;}' foreach:car ", type=ExampleType.EXECUTABLE)
	@Example(expr="[ ferry:Orcas,car:[weight:15],car:[weight:4] ] [ tooHeavyCarExists=or:'{l.weight > 20L;}' foreach:car ", type=ExampleType.EXECUTABLE)
	@Example(expr="[ book:harryPotter,review:[iLoveIt:true],review:[iLoveIt:false] ] [ someoneLovesIt=or:iLoveIt foreach:review ", type=ExampleType.EXECUTABLE)	
    public static class OrOp extends ReducerOperator {
		public OrOp() {
			super("OUTPUT=or:INPUT");
		}
	
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            return new BooleanReducer(null, '|', args).addSource(operands.pop());
        }
	}
	
	/**
	 * 
	 *
	 */
	@Description(text={"And's INPUT into OUTPUT. Mainly used to propagate information from child records into the parent."})
    @Arg(name="INPUT", gloss="the VALUE expression which must evaulate to boolean (see VALUE)", type=ArgType.VALUE)
	@Arg(name="OUTPUT", gloss="output field of the reduced record", type=ArgType.FIELD)
	@Example(expr="[ ferry:Orcas,car:[weight:15],car:[weight:40],car:[weight:4] ] [ allCarsAreLight=and:'{l.weight < 20L;}' foreach:car ", type=ExampleType.EXECUTABLE)
	@Example(expr="[ ferry:Orcas,car:[weight:15],car:[weight:4] ] [ allCarsAreLight=and:'{l.weight < 20L;}' foreach:car ", type=ExampleType.EXECUTABLE)
	@Example(expr="[ book:harryPotter,review:[iLoveIt:true],review:[iLoveIt:false] ] [ everyoneLovesIt=and:iLoveIt foreach:review ", type=ExampleType.EXECUTABLE)	
    public static class AndOp extends ReducerOperator {
		public AndOp() {
			super("OUTPUT=and:INPUT");
		}
	
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            return new BooleanReducer(null, '&', args).addSource(operands.pop());
        }
	}
}

