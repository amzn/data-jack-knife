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
import com.amazon.djk.misc.Hashing;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.FieldBytesRef;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

import java.io.IOException;

@ReportFormats(headerFormat="<output>%s=hash:<args>%s")
public class HashPipe extends RecordPipe {
    private static final String USE_64_BIT_PARAM = "64bit";
    private static final String MOD_PARAM = "mod";
    private static final String DEFAULT_MOD_PARAM = "0";
    private static final String DEFAULT_64_BIT_PARAM = "false";

    @ScalarProgress(name="args")
    private final OpArgs args;    
    private final Fields inputs;
    @ScalarProgress(name="output")
	private final Field output;

    private final int seed;

    private final FieldIterator inputIter;

    private final long mod;
    private final boolean use64;
    private final FieldBytesRef valueRef = new FieldBytesRef();

    public HashPipe(HashPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        this.inputs = (Fields)args.getArg("INPUTS");
        this.output = (Field)args.getArg("OUTPUT");
        this.seed = (int) args.getParam("seed");
        this.mod = (Long)args.getParam(MOD_PARAM);
        this.use64 = (Boolean)args.getParam(USE_64_BIT_PARAM);
        inputIter = inputs.getAsIterator();
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new HashPipe(this, args);
    }
    
    @Override
    public Record next() throws IOException {
    	Record rec = super.next();
    	if (rec == null) return null;

    	inputIter.init(rec);
    	if (!inputIter.next()) {
    		return rec;
    	}

    	inputIter.getValueAsBytesRef(valueRef);
    	long hash = use64 ? Hashing.hash64(valueRef, seed) : Hashing.hash63(valueRef, seed);

    	while (inputIter.next()) {
    		inputIter.getValueAsBytesRef(valueRef);
    		long temp = use64 ? Hashing.hash64(valueRef, seed) : Hashing.hash63(valueRef, seed);
            hash ^= temp; // xor
    	}
    	
    	rec.addField(output, mod < 2 ? hash : hash % mod);
    	
    	return rec;
    }
	
    @Description(text={"Creates a non-negative long hash value in OUTPUT based on the INPUT field."})
    @Arg(name="INPUTS", gloss="fields to create hash from.", type=ArgType.FIELDS)
    @Arg(name="OUTPUT", gloss="output field for the hash value.", type=ArgType.FIELD)
    @Param(name= MOD_PARAM, gloss="perform modulo on the hash, i.e. 'hash % mod'.", type=ArgType.LONG, defaultValue=DEFAULT_MOD_PARAM)
    @Param(name= USE_64_BIT_PARAM, gloss="employ a 64-bit hash (allows negative values).", type=ArgType.BOOLEAN, defaultValue=DEFAULT_64_BIT_PARAM)
    @Param(name = "seed", gloss = "seed to use for the hash", type = ArgType.INTEGER, defaultValue = "0")
    @Example(expr = "[ hello:world ] myhash=hash:hello", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("OUTPUT=hash:INPUTS");
    	}

        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            return new HashPipe(null, args).addSource(operands.pop());
        }
    }
}
