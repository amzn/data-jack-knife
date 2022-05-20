package com.amazon.djk.pipe;

import java.io.IOException;

import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Operator;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.NotIterator;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

/**
 * records will retain only those fields specified as the argument.
 *
 */
@ReportFormats(headerFormat="<fields>%s")
public class KeepFieldsPipe extends RecordPipe {
	public static final String NAME = "keep";
    @ScalarProgress(name="fields")
	private final Fields fields;
	private final NotIterator notFields;
    
    public KeepFieldsPipe(Fields fields) throws IOException {
        this(null, fields);
    }
	
    public KeepFieldsPipe(KeepFieldsPipe root, Fields fields) throws IOException {
        super(root);
        this.fields = fields;
        notFields = !fields.isAllFields() ? fields.getAsNotIterator() : null;
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new KeepFieldsPipe(fields);
    }
    
    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;

        if (notFields == null) {
        	return rec; // keep all
        }
        
        notFields.init(rec);
        while (notFields.next()) {
        	rec.deleteField(notFields);
        }
        
        return rec;
    }
    
    /**
     * 
     * @return
     */
    @Description(text={"records will retain only the fields in FIELDS."})
    @Arg(name="FIELDS", gloss="list of fields to retain. Use '+' to keep all fields.", type=ArgType.FIELDS)
    @Example(expr="[ color:blue,size:big,where:up ] keep:color", type=ExampleType.EXECUTABLE)
    @Example(expr="[ color:blue,size:big,where:up ] keep:+", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super(NAME + ":FIELDS");
        }

        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            Fields fields = (Fields)args.getArg("FIELDS");
            if (fields.isSpecifiedAsNegative()) {
                throw new SyntaxError("Illegal to specify negative fields. Use 'rm' predicate");
            }
            return new KeepFieldsPipe(fields).addSource(operands.pop());
        }
    }
}
