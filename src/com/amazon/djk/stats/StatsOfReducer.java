package com.amazon.djk.stats;

import java.io.IOException;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.pipe.BinValuePipe;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.reducer.Reducer;
import com.amazon.djk.reducer.ReducerOperator;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

@ReportFormats(headerFormat="<args>%s")
public class StatsOfReducer extends Reducer {
    private static final String MAX_POINTS = "maxPoints";
    private static final String MIN_COUNT = "minCount";
    private static final String DOUBLE_BIN = "doubleBin";
    private static final String DEFAULT_MAX_POINTS = "10";
    private static final String DEFAULT_MIN_COUNT = "1";
    private static final String DEFAULT_DOUBLE_BIN = "0.01";
    private static final String SUB_STATS_BY = "subStatsBy";

    private final StatsHolder stats;
    private final Record outrec = new Record();
    private final double doubleBinSize;

    @ScalarProgress(name = "args")
    private final OpArgs args;
    

    private final Fields fields;
    private final FieldIterator fiter;

    private final Fields subStatsByFields;
    private final FieldIterator subsStatsByFieldIterator;

    public StatsOfReducer(OpArgs args) throws IOException {
        super(null, args, Type.MAIN_ONLY);
        this.args = args;
        fields = (Fields)args.getArg("INPUTS");
        subStatsByFields = (Fields)args.getParam(SUB_STATS_BY);
        long maxPoints = (long)args.getParam(MAX_POINTS);
        long minPointCount = (long)args.getParam(MIN_COUNT);
        doubleBinSize = (double)args.getParam(DOUBLE_BIN);

        if (maxPoints == -1) {
            maxPoints = Long.MAX_VALUE;
        }
        fiter = fields.getAsIterator();
        subsStatsByFieldIterator = subStatsByFields == null ? null : subStatsByFields.getAsIterator();
        stats = new StatsHolder(maxPoints, minPointCount);
    }
    
    /**
     * constructor for:
     * <p>
     * replicate mainExpression
     * getAsPipe mainExpression
     * subReplicate subExpression
     * </p>
     *
     * @throws IOException
     */
    private StatsOfReducer(StatsOfReducer root, OpArgs args, StatsHolder stats) throws IOException {
        super(root, args, Type.MAIN_ONLY);
        this.args = args;
        fields = (Fields)args.getArg("INPUTS");
        subStatsByFields = (Fields)args.getParam(SUB_STATS_BY);
        doubleBinSize = (double)args.getParam(DOUBLE_BIN);
        this.stats = stats;
        fiter = fields.getAsIterator();
        subsStatsByFieldIterator = subStatsByFields == null ? null : subStatsByFields.getAsIterator();
    }

    @Override
    public Object replicate() throws IOException {
        return new StatsOfReducer(this, args, stats);
    }
    
    @Override
    public Record getChildReduction() throws IOException {
        return stats.getChildReduction(getInstanceName());
    }
    
    @Override
    public Record getNextMainReduction() throws IOException {
    	return stats.getNextMainReduction(getInstanceName());
    }

    @Override
    public boolean reset() {
        outrec.reset();
        return true;
    }

    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;

        stats.incNumRecs();
        fiter.init(rec);

        while (fiter.next()) {
            increaseCounter(fiter.getName());
            addGranularStats(rec);
        }

        return rec;
    }

    private void addGranularStats(Record rec)  throws  IOException{
        if (subStatsByFields != null) {
            subsStatsByFieldIterator.init(rec);
            while (subsStatsByFieldIterator.next()) {
                increaseCounter(String.format("%s_%s", fiter.getName() , subsStatsByFieldIterator.getValueAsString()));
            }
        }
    }

    private void increaseCounter(String fieldName) throws IOException {
        try {
            switch (fiter.getType()) {
                case STRING:
                    String sval = fiter.getValueAsString();
                    stats.addString(fieldName, sval);
                    break;

                case LONG:
                    Long lval = fiter.getValueAsLong();
                    stats.addLong(fieldName, lval);
                    break;

                case DOUBLE:
                    Double dval = fiter.getValueAsDouble();
                    dval = BinValuePipe.bin(dval, doubleBinSize);
                    stats.addDouble(fieldName, dval);
                    break;

                case BOOLEAN:
                    Boolean bval = fiter.getValueAsBoolean();
                    stats.addBoolean(fieldName, bval);
                    break;

                case BYTES:
                    break;

                case ERROR:
                    break;

                case NULL:
                    break;

                case RECORD:
                    break;

                default:
                    break;
            }
        }

        catch (ClassCastException e) {
            throw new SyntaxError(String.format("Not all values of field '%s' are the same type.  If parsing from nv2, use '<TYPE> %s;' where <TYPE> = STRING,DOUBLE,LONG or BOOLEAN", fieldName, fieldName));
        }
    }

    @Description(text = {"Generates stats over the specified record fields."})
    @Arg(name= "INPUTS", gloss="comma separated list of fields", type=ArgType.FIELDS)
    @Param(name= MAX_POINTS, gloss="maximum number of histogram points to return. no-max=-1", type=ArgType.LONG, defaultValue = DEFAULT_MAX_POINTS)
    @Param(name= MIN_COUNT, gloss="minimum number point instances required to be included.", type=ArgType.LONG, defaultValue = DEFAULT_MIN_COUNT)
    @Param(name= DOUBLE_BIN, gloss="size of bin used to bin double values. see 'bin'", type=ArgType.DOUBLE, defaultValue = DEFAULT_DOUBLE_BIN)
    @Param(name= SUB_STATS_BY, gloss = "the sub stats will be calculated for the INPUT fields against these fields", type = ArgType.FIELDS)
    @Example(expr =  "[ id:1,color:red,size:m id:2,color:red,size:s id:3,color:blue,size:s ] statsOf:color devnull ", type = ExampleType.EXECUTABLE)
    @Example(expr =  "[ id:1,color:red,size:m id:2,color:red,size:s id:3,color:blue,size:s ] statsOf:color?subStatsBy=size devnull ", type = ExampleType.EXECUTABLE)
    public static class Op extends ReducerOperator {
        public Op() {
            super("statsOf:INPUTS");
        }

        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException {
            return new StatsOfReducer(args).addSource(operands.pop());
        }
    }
}
