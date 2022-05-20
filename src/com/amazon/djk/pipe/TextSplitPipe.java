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
import com.amazon.djk.record.Field;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.UTF8BytesRef;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

import java.io.IOException;

@ReportFormats(headerFormat="fields=%s"
)
public class TextSplitPipe extends RecordPipe {
    private final static String TEXT_FIELD = "text";
	private final static String FIELD_FIELD = "field";
    private static final String CHILD = "child";
    private static final String WITH_FIELD = "withField";
    private static final String DEFAULT_CHILD_FIELD = "term";
    private static final String DEFAULT_WITH_FIELD_PARAM = "false";
    private static final String INPUTS = "INPUTS";


    @ScalarProgress(name="fields")
	private final Fields fields;
	private final FieldIterator fiter;

	private final OpArgs args;
	private final UTF8BytesRef stringRef = new UTF8BytesRef();
	private final UTF8BytesRef tokenRef = new UTF8BytesRef();
    private final Record subrec = new Record();
    private final Field child;
    private final boolean withField;
    
    public TextSplitPipe(TextSplitPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        this.fields = (Fields)args.getArg(INPUTS);
        this.fiter = fields.getAsIterator();
        this.child = (Field)args.getParam(CHILD);
        this.withField = (Boolean)args.getParam(WITH_FIELD);
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new TextSplitPipe(this, args);
    }
    
    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;

        fiter.init(rec);
        while (fiter.next()) {
            if (fiter.getValueAsUTF8BytesRef(stringRef)) { // if is STRING
                stringRef.whitespaceTokenizeInit();

                while (stringRef.nextWhitespaceToken(tokenRef)) {
                    subrec.reset();
                    subrec.addField(TEXT_FIELD, tokenRef);
                    if (withField) {
                        subrec.addField(FIELD_FIELD, fiter.getName());
                    }
                    rec.addField(child, subrec);
                }
            }

            else { // else NonString
                String temp = fiter.getValueAsString();
                subrec.reset();
                subrec.addField(TEXT_FIELD, temp);
                if (withField) {
                    subrec.addField(FIELD_FIELD, fiter.getName());
                }
                rec.addField(child, subrec);
            }
        }
        
        return rec;
    }
    
    @Description(text={"Txtsplit assumes white-spaced tokenization on the input text fields.",
    		"For each field in FIELDS it adds child record 'term' with fields 'text' and 'field'"})
    @Arg(name=INPUTS, gloss="comma separated list of fields to tokenize.", type=ArgType.FIELDS, eg="title")
    @Param(name= CHILD, gloss="name of child record.", type=ArgType.FIELD, defaultValue = DEFAULT_CHILD_FIELD)
    @Param(name= WITH_FIELD, gloss="if true the source field is added to the child record.", type=ArgType.BOOLEAN, defaultValue = DEFAULT_WITH_FIELD_PARAM)
    @Example(expr="[ title:'colorless green ideas',brand:chomsky ] txtsplit:title,brand", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {

        public Op() {
        	super("txtsplit:INPUTS");
        }
        
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            return new TextSplitPipe(null, args).addSource(operands.pop());
        }
    }
}
