package com.amazon.djk.pipe;

import java.io.IOException;

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
import com.amazon.djk.record.UTF8BytesRef;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

@ReportFormats(headerFormat="<output>%s=<input>%s")
public class TextCountToksPipe extends RecordPipe {
    private final OpArgs args;
    @ScalarProgress(name="input")
    private final Field input;
    @ScalarProgress(name="output")
    private final Field output;
	
    private final UTF8BytesRef utf8Ref = new UTF8BytesRef();
    private final UTF8BytesRef utf8Token = new UTF8BytesRef();
    
    public TextCountToksPipe(TextCountToksPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        input = (Field)args.getArg("INPUT");
        output = (Field)args.getArg("OUTPUT");
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new TextCountToksPipe(this, args);
    }
    
    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;

        input.init(rec);
        if (!input.next()) return rec; // bail 

        input.getValueAsUTF8BytesRef(utf8Ref);
        
        long numToks = 0;
        utf8Ref.whitespaceTokenizeInit();
        while (utf8Ref.nextWhitespaceToken(utf8Token)) {
            numToks++;
        }

        rec.addField(output, numToks); 

        return rec;
    }

    @Description(text={"Counts the number of whitespace separated tokens. (don't have double spaces)"})
    @Arg(name="INPUT", gloss="count the tokens of this field", type=ArgType.FIELD)
    @Arg(name="OUTPUT", gloss="output field for the count", type=ArgType.FIELD)
    @Example(expr="[ id:1,text:'one two three' ] count=txtcount:text", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
        public Op() {
        	super("OUTPUT=txtcount:INPUT");
        }
        
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
        	return new TextCountToksPipe(null, args).addSource(operands.pop());
        }
    }
}
