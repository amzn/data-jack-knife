package com.amazon.djk.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;
import com.amazon.djk.reducer.LazyReductionSource;
import com.amazon.djk.reducer.Reducer;
import com.amazon.djk.reducer.ReducerOperator;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

/**
 * 
 *
 */
@ReportFormats(headerFormat="<args>%s")
public class DivergenceReducer extends Reducer {
	public final static String FIELD_FIELD = "field";

	@ScalarProgress(name = "args")
	private final OpArgs args;
	
	private final Fields fields;
    private Map<String,DivergenceCalculator> calculators = new HashMap<>();
    
    /**
     * main constructor 
     * @param root
     * @param args
     * @throws IOException
     */
    private DivergenceReducer(DivergenceReducer root, OpArgs args) throws IOException {
        super(root, args, Type.MAIN_ONLY);
        this.args = args;
        fields = (Fields)args.getArg("STATS");
        List<Field> fieldList = fields.getAsFieldList(); 
        for (Field field : fieldList) {
        	calculators.put(field.getName(), new DivergenceCalculator(field));
        }
    }
    
    // purposefully does not replicate
    
	@Override
	public Record getChildReduction() throws IOException {
		Record results = new Record();
		for (String key : calculators.keySet()) {
			DivergenceCalculator calculator = calculators.get(key);
			Record result = calculator.getResult();
			if (result != null) {
				results.addField(key, result);
			}
		}
		
		return results;
	}
    
    @Override
    public Record next() throws IOException {
    	Record rec = super.next();
    	if (rec == null) return null;
    	
        String value = rec.getFirstAsString(FIELD_FIELD); // contains statsOf field
        if (value == null) return rec;
        
        DivergenceCalculator calculator = calculators.get(value);
        if (calculator != null) {
        	calculator.offer(rec);
        }

        return rec;
    }

    @Override
    public boolean reset() {
    	return true; // nothing to do
    }

    @Description(text={"Calculates the cross-entropy and Kullback-Leibler Divergence of two distributions as output by 'statsOf'.",
    		"The first two record instances with a STATS field are used for the calculation.  The record instances can be ",
    		"made distinguishable using the 'instance' param of 'statsOf' or by adding arbitrary fields to the records.",
    		"Currently, reduction is non-recursive so two passes are required."})
    @Arg(name="STATS", gloss="'statsOf' fields, over which to calculate divergence.", type=ArgType.FIELDS)
    public static class Op extends ReducerOperator {
    	public Op() {
    		super("divergence:STATS");
    	}
        	
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		return new DivergenceReducer(null, args).addSource(operands.pop());
    	}
    }
}
