package com.amazon.djk.core;

import com.amazon.djk.core.Denormalizer.AddMode;
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
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

import java.io.IOException;

@ReportFormats(headerFormat = "<args>%s", lineFormats = {"count=%,d"})
public class DenormalizePipe extends RecordPipe implements Keyword {
    @ScalarProgress(name = "args")
    private final OpArgs args;
    private final Denormalizer denormer;
    private final boolean keepEmpty;

    @ScalarProgress(name = "count")
    private long count = 0;

    public DenormalizePipe(OpArgs args) throws IOException {
        this(null, args);
    }

    private DenormalizePipe(RecordPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        Field subrec = (Field) args.getArg("CHILD");
        boolean childOnly = (Boolean) args.getParam("childOnly");
        keepEmpty = (Boolean) args.getParam("keepEmpty") && !childOnly;

        denormer = new Denormalizer(subrec.getName(), childOnly ?
                AddMode.CHILD_FIELDS_ONLY :
                AddMode.INCLUDE_REMNANT_FIELDS,
                Denormalizer.Context.SIMPLE_DENORM);
    }

    @Override
    public Object replicate() throws IOException {
        return new DenormalizePipe(args);
    }

    @Override
    public Record next() throws IOException {
        Record out = denormer.next();

        while (out == null) {
            Record in = super.next();
            if (in == null) return null;
            denormer.init(in);
            out = denormer.next();

            // if no children, return the incoming childless record
            if (out == null && keepEmpty) {
                out = in;
            }
        }

        count++;

        return out;
    }

    @Description(text = {"Denormalizes sub-records or fields of the input source.  For each CHILD sub-record/field of an incoming record,",
            "an output record is produced, which either includes or excludes the parent fields according to",
            "the 'childOnly' parameter.  Incoming records without a CHILD produce no output records unless",
            "'keepEmpty' is true.  When CHILD is a field instead of sub-record, then the field itself is added to the parent."})
    @Arg(name = "CHILD", gloss = "name of the sub-record or field to denormalize", type = ArgType.FIELD)
    @Param(name = "childOnly", gloss = "denormalizes sub-records without the parent fields", type = ArgType.BOOLEAN, defaultValue = "false")
    @Param(name = "keepEmpty", gloss = "if true and childOnly=false, childless input records are passed through to the output.", type = ArgType.BOOLEAN, defaultValue = "false")
    @Example(expr = "[ ferry:orca,car:[make:honda],car:[make:toyota] ferry:chelan ] denorm:car", type = ExampleType.EXECUTABLE)
    @Example(expr = "[ ferry:orca,car:[make:honda],car:[make:toyota] ferry:chelan ] denorm:car'?keepEmpty=true'", type = ExampleType.EXECUTABLE)
    @Example(expr = "[ ferry:orca,car:[make:honda],car:[make:toyota] ferry:chelan ] denorm:car'?childOnly=true'", type = ExampleType.EXECUTABLE)
    @Example(expr = "[ foo:bar,id:1,id:2,id:3 ] denorm:id", type = ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
        public Op() {
            super("denorm:CHILD");
        }

        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            return new DenormalizePipe(args).addSource(operands.pop());
        }
    }
}
