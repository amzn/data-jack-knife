package com.amazon.djk.pipe;

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
import com.amazon.djk.report.ReportFormats;

import java.io.IOException;

@ReportFormats(headerFormat="<conditional>%s", lineFormats={
        "accepted=%,d (<percentAccepted>%2.1f%%) rejected=%,d (<percentRejected>%2.1f%%) nonBoolean=%,d"
})
public class AcceptIf extends RejectIf {
	
    public AcceptIf(OpArgs args) throws IOException {
        super(args);
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new AcceptIf(args);
    }

    @Override
    protected boolean getConditional(Record curr) throws IOException {
        // we are overriding the conditional for RejectIf.
        // therefore nonBoolean and NullPointer return true.
        Object obj = value.getValue(curr);
        if (! (obj instanceof Boolean) ) {
            numNonBools++;
            return true;
        }
            
        return !(Boolean) obj;
    }
    
    /**
     * 
     * @return
     */
    public static PipeOperator getOperator() {
    	return new Op();
    }
    
    @Description(text={"Keeps records for which VALUE evaluates to BOOLEAN true.",
            "If a field does not evaluate to BOOLEAN or is nonexistent, the conditional evaluates to false (and nonBoolean count increments).",
            "in this way it's possible to evaluate sources where not every record contains a field.  See 'rejectIf'"})
    @Arg(name="CONDITIONAL", gloss="a conditional expression evaluates to a boolean (see 'man VALUE')", type=ArgType.VALUE)
    @Param(name="logEach", gloss="LEVEL, where LEVEL = {ERROR|INFO|WARN|OFF}. Causes a log entry for each rejected record.", type=ArgType.STRING, defaultValue = DEFAULT_LOG_LEVEL)
    @Example(expr="[ id:1,color:red id:2,color:blue ] acceptIf:'{l.id == 2;}'", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1 id:2 color:blue ] acceptIf:'{l.id == 2;}'", type=ExampleType.EXECUTABLE_GRAPHED)
    @Example(expr="[ id:1,color:red id:2,color:blue ] acceptIf:'{s.color.equals(\"blue\");}'", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1,color:red id:2,color:blue ] acceptIf:'{R.fieldsSubsetOf(\"id,color,brand\");}'", type=ExampleType.EXECUTABLE)    
    @Example(expr="[ id:1,color:red id:2,color:blue ] acceptIf:'{R.fieldsSupersetOf(\"id,color,brand\");}'", type=ExampleType.EXECUTABLE)    
    public static class Op extends PipeOperator {
        public Op() {
        	super("acceptIf:CONDITIONAL");
        }
        
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
        	return new AcceptIf(args).addSource(operands.pop()); 
        }
    }
}
