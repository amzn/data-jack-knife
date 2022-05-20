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
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

import java.io.IOException;

@ReportFormats(lineFormats={
		"field=%s binSize=%f"
})
public class BinValuePipe extends RecordPipe {
    public static final String BINSIZE = "BINSIZE";
    public static final String FIELD = "FIELD";
    private final OpArgs args;
	@ScalarProgress(name="binSize")
	private final double binSize;
	@ScalarProgress(name="field")
    private final Field field;

	public BinValuePipe(OpArgs args) throws IOException {
	    this(null, args);
	}
	
    private BinValuePipe(BinValuePipe root, OpArgs args) throws IOException {
        super(root);
    	this.args = args;
    	this.field = (Field)args.getArg(FIELD);
        this.binSize = (double)args.getArg(BINSIZE);
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new BinValuePipe(this, args);
    }
    
    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;

        // get both long and double as double
        Double value = rec.getFirstAsDouble(field);
        if (value == null) return rec;
        
        long valueAsLong = (long)(value / binSize);
        if (binSize >= 1) {
            valueAsLong *= binSize;
        	rec.updateField(field, valueAsLong);
		} else {
			double val = (double)(valueAsLong * binSize); // rescale
			rec.updateField(field, val);
		}

        return rec;
    }

    public static double bin(double value, double binSize) {
        long valueAsLong = (long)(value / binSize);
        return (double)(valueAsLong * binSize); // rescale
    }
    
    public static long bin(long value, double binSize) {
        long valueAsLong = (long)(value / binSize);
        valueAsLong *= binSize;
        return valueAsLong;
    }
    
    /**
     * 
     * @return
     */
    public static PipeOperator getOperator() {
    	return new Op();
    }
    
    @Description(text={"numerically bins values in bins of a size specified by BINSIZE."})
    @Arg(name=FIELD, gloss="the numeric field to bin", type=ArgType.FIELD)
    @Arg(name=BINSIZE, gloss="the size of the bins", type=ArgType.DOUBLE)
    @Example(expr="[ num:1324 num:23 ] bin:num:10", type=ExampleType.EXECUTABLE)
    @Example(expr="[ num:0.0331 num:0.546 ] bin:num:0.02", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
        public Op() {
        	super("bin:FIELD:BINSIZE");
        }
        
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
        	double binSize = (double)args.getArg(BINSIZE);
            if (binSize < 0.000000001) {
        		throw new SyntaxError("bin size too small");
        	}

        	return new BinValuePipe(args).addSource(operands.pop()); 
        }
    }
}
