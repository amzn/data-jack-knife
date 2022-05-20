package com.amazon.djk.concurrent;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
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
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;

import java.io.IOException;

@ReportFormats(headerFormat="<fields>%s<paramsDisplay>%s", 
    lineFormats={"mapSize=%,d"})
public class UniquePipe extends RecordPipe {
    public final static String RETAIN_PARAM = "retainOthers";
    public final static String COUNT_PARAM = "count";
    public final static String INPUT_FIELDS_ARG = "INPUTS";
    public final static String DEFAULT_COUNT_PARAM = "false";
    public final static String DEFAULT_RETAIN_PARAM = "false";

    @ScalarProgress(name="fields")
	private final FieldIterator fields;
	@ScalarProgress(name="mapSize", aggregate=AggType.NONE)
	private long mapSize;
	private final RecordCounter counter;
	private final OpArgs args;
	
	@ScalarProgress(name="paramsDisplay")
	private String paramsDisplay = "";
	private final boolean isInSubExpression;
	
    private boolean isExhausted = false;
	
	/**
	 * constructor for:
	 * 
	 * getAsPipe mainExpression
	 * subReplicate subExpression
	 * @throws IOException 
	 *  
	 */
    public UniquePipe(OpArgs args) throws IOException {
    	this(null, args, new RecordCounter(args));
    }
    
	/**
	 * map and sync shared for mainExpression level
	 * 
	 * replicate mainExpression
	 * @throws IOException 
	 */
    private UniquePipe(RecordPipe root, OpArgs args, RecordCounter counter) throws IOException {
        super(root);
    	Fields fields = (Fields)args.getArg(INPUT_FIELDS_ARG);
    	this.fields = fields.getAsIterator();
    	this.args = args;
    	this.isInSubExpression = args.isInSubExpression();
    	this.counter = counter;
    	
    	paramsDisplay = (Boolean)args.getParam(RETAIN_PARAM) ?
    	        "?retainOthers=true" : "";
    	paramsDisplay = (Boolean)args.getParam(COUNT_PARAM) ?
    	        paramsDisplay + "&count=true" : paramsDisplay;
    }
	
    @Override
    public Object replicate() throws IOException {
    	return new UniquePipe(this, args, (RecordCounter)counter.replicate());
    }
    
    @Override
    public Object subReplicate() throws IOException {
    	return new UniquePipe(this, args, new RecordCounter(args));
    }
    
    @Override
    public Record next() throws IOException {
        loadCounter(counter);
    	return counter.next();
    }
    
    /**
     * 
     * @throws IOException
     */
    
    /**
     * exhausts the input source into the counter
     * 
     * @param counter
     * @return true if this thread is done
     * @throws IOException
     */
    private void loadCounter(RecordCounter counter) throws IOException {
    	if (isExhausted) return;
    	
    	while (!isExhausted) {
    		Record rec = super.next();
    		if (rec == null) {
        		mapSize = counter.size();
        		isExhausted = true;
    		}
    		
    		else {
    			counter.count(rec);
    			mapSize = counter.size();
    		}
    	}
    
    	// synchronize the mainLevelExpression only
    	if (isInSubExpression || isLastSync1.arriveAndIsLast()) {
    	    counter.finish();               
    	}
    }
    
    @Override
    public boolean reset() {
    	isExhausted = false;
		counter.reset();
    	return true;
    }
	
    private final static String expr1 = 
    		"[ title:'one two two three three three' ] txtsplit:title [ uniq:text foreach:term";
    private final static String expr2 = 
    		"[ title:'one two two three three three' ] txtsplit:title [ uniq:text'?count=true' foreach:term";
    
    @Description(text={"Uniques records with respect to INPUTS. Other fields will be removed unless retainOthers=true"})
    @Arg(name="INPUTS", gloss = "the input fields to unique by", type = ArgType.FIELDS)
    @Param(name=COUNT_PARAM, gloss="If 'true', behaves like unix uniq -c.", type=ArgType.BOOLEAN, defaultValue = DEFAULT_COUNT_PARAM)
    @Param(name=RETAIN_PARAM, gloss="If 'true', the other fields of the first instance of a record are retained", type=ArgType.BOOLEAN, defaultValue = DEFAULT_RETAIN_PARAM)
    @Example(expr=expr1, type=ExampleType.EXECUTABLE)
    @Example(expr=expr2, type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("uniq:INPUTS");
    	}
    	
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            return new UniquePipe(args).addSource(operands.pop());
        }
    }
}
