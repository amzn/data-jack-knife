package com.amazon.djk.pipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

@ReportFormats(headerFormat="fields=%s")
public class MergeFieldPipe extends RecordPipe {
    @ScalarProgress(name="fields")
	private final List<Field> fields = new ArrayList<>();
	private final OpArgs args;
	private final StringBuilder sb = new StringBuilder();
	
    public MergeFieldPipe(OpArgs args) throws IOException {
        this(null, args);
    }
	
    /**
     * replica constructor 
     * @param root
     * @param args
     * @throws IOException
     */
    private MergeFieldPipe(MergeFieldPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        Fields fieldsObj = (Fields)args.getArg("FIELDS");

        List<String> list = fieldsObj.getFieldNames();
        for (String name : list) {
        	fields.add(new Field(name));
        }
    }
	
    @Override
    public Object replicate() throws IOException {
        return new MergeFieldPipe(this, args);
    }
    
    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;

        for (Field field : fields) {
        	sb.setLength(0);
        	field.init(rec);
        	while (field.next()) {
        		if (sb.length() != 0) {
        			sb.append(' ');
        		}
        		rec.deleteField(field);
        		sb.append(field.getValueAsString());
        	}
        	
        	if (sb.length() != 0) {
        		rec.addField(field.getName(), sb.toString());
        	}
        }

        return rec;
    }
    
    @Override
    public boolean reset() {
    	return true; // nothing to do
    }

    @Description(text={"Merges all the instances of field of a given name, space separated, into a single instance.",
    		"Multiple field names may be provided, each of which will be merged into a single instance",
    		"(does not merge across fields of a given name.)"})
    @Arg(name="FIELDS", gloss="the name of the fields to merge", type=ArgType.FIELDS)
    @Example(expr="[ id:1,foo:cat,foo:12,foo:3.40,size:big ] merge:foo,size", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("merge:FIELDS");
    	}
        	
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		return new MergeFieldPipe(args).addSource(operands.pop());
    	}
    }
}
