package com.amazon.djk.pipe;

import com.amazon.djk.core.Keyword;
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
import com.amazon.djk.record.Record;
import com.amazon.djk.record.Value;
import com.amazon.djk.report.PercentProgress;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import org.apache.logging.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@ReportFormats(headerFormat="<conditional>%s", lineFormats={
        "accepted=%,d (<percentAccepted>%2.1f%%) rejected=%,d (<percentRejected>%2.1f%%) nonBoolean=%,d"
})
public class RejectIf extends RecordPipe implements Keyword {
    private static final Logger logger = LoggerFactory.getLogger(RejectIf.class);
    public static final String DEFAULT_LOG_LEVEL = "OFF";

    protected final OpArgs args;
	
	@ScalarProgress(name="conditional")
    protected final Value value;
    @ScalarProgress(name="nonBoolean")
    protected volatile long numNonBools = 0;
    
    @ScalarProgress(name = "inRecs")
    private volatile long inRecs = 0;
    
    @PercentProgress(name = "percentRejected", denominatorAnnotation = "inRecs")
    @ScalarProgress(name="rejected")
    private volatile long skipCount = 0;
    
    @PercentProgress(name = "percentAccepted", denominatorAnnotation = "inRecs")
    @ScalarProgress(name="accepted")
    private volatile long keptCount = 0;
    
    private final Level logLevel;
	
    public RejectIf(OpArgs args) throws IOException {
        this(null, args);
    }
    
    public RejectIf(RejectIf root, OpArgs args) throws IOException {
        super(root);
    	this.args = args;
    	this.value = (Value)args.getArg("CONDITIONAL");
    	logLevel = Level.toLevel((String)args.getParam("logEach"));
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new RejectIf(this, args);
    }
    
    protected boolean getConditional(Record curr) throws IOException {
        Object obj = value.getValue(curr);
        if (! (obj instanceof Boolean) ) {
            numNonBools++;
            return false;
        }

        return (Boolean) obj;
    }
    
    @Override
    public Record next() throws IOException {
        while (true) {
            Record rec = super.next();
            if (rec == null) return null;

            inRecs++;
        
            if (getConditional(rec)) {
                skipCount++;
                
                if (!logLevel.equals(Level.OFF)) {
                    log(rec);
                }
                continue;                
            }
            
            else {
                keptCount++;
                return rec;                
            }
        }
    }

    private void log(Record record) {
        switch (logLevel.getStandardLevel()) {

        case ERROR:
            logger.error(String.format("rejected record (%s) where (%s)", record, value.getDisplayString()));
            return;

        case WARN:
            logger.warn(String.format("rejected record (%s) where (%s)", record, value.getDisplayString()));
            return;

        default:
            logger.info(String.format("rejected record (%s) where (%s)", record, value.getDisplayString()));
            return;
        }
    }
    
    /**
     * 
     * @return
     */
    public static PipeOperator getOperator() {
    	return new Op();
    }
    
    @Description(text={"Skips records for which VALUE evaluates to BOOLEAN true.",
            "If a field does not evaluate to BOOLEAN or is nonexistent, the conditional evaluates to false (and nonBoolean count increments).",
            "in this way it's possible to evaluate sources where not every record contains a field.  See 'acceptIf'"
            })
    @Arg(name="CONDITIONAL", gloss="a conditional expression evaluates to a boolean (see 'man VALUE')", type=ArgType.VALUE)
    @Param(name="logEach", gloss="LEVEL. Where LEVEL = {ERROR|INFO|WARN|OFF}. Causes a log entry for each rejected record.", type=ArgType.STRING, defaultValue = DEFAULT_LOG_LEVEL)
    @Example(expr="[ id:1,color:red id:2,color:blue ] rejectIf:'{l.id == 2;}'", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1 id:2 color:blue ] rejectIf:'{l.id == 2;}'", type=ExampleType.EXECUTABLE_GRAPHED)
    @Example(expr="[ id:1,color:red id:2,color:blue ] rejectIf:'{s.color.equals(\"blue\");}'", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1,color:red id:2,color:blue ] rejectIf:'{R.fieldsSubsetOf(\"id,color,brand\");}'", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1,color:red id:2,color:blue ] rejectIf:'{R.fieldsSupersetOf(\"id,color,brand\");}'", type=ExampleType.EXECUTABLE)        
    public static class Op extends PipeOperator {

        public Op() {
        	super("rejectIf:CONDITIONAL");
        }
        
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
        	return new RejectIf(args).addSource(operands.pop()); 
        }
    }
}
