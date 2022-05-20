package com.amazon.djk.source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazon.djk.core.BaseRecordSource;
import com.amazon.djk.core.Denormalizer;
import com.amazon.djk.core.Denormalizer.AddMode;
import com.amazon.djk.core.Denormalizer.Context;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;
import com.amazon.djk.sink.FileSinkHelper;

/**
 *
 */
@ReportFormats(headerFormat = "<source>%s")
public class FormatOriginSource extends BaseRecordSource {
    public final static String FORMAT = "origin";
    @ScalarProgress(name = "source", aggregate = AggType.NONE)
    private final String path;
    private final boolean flatten;
    private final List<Record> list = new ArrayList<>();

    public FormatOriginSource(OpArgs args) throws IOException {
        path = (String) args.getArg("PATH");
        flatten = (Boolean) args.getParam("flatten");

        File sourceDir = new File(path);
        Record originReport = FileSinkHelper.getOriginReport(sourceDir);
        if (originReport == null) {
            Record origin = new Record();
            origin.addField("WARNING", "origin report missing");
            list.add(origin);
        } else if (flatten) {
            flatten(originReport);
        } else {
            list.add(originReport);
        }
    }

    private void flatten(Record node) throws IOException {
        Denormalizer denormer = new Denormalizer("node", AddMode.CHILD_FIELDS_ONLY, Context.SIMPLE_DENORM);
        Record parent = denormer.init(node);
        if (parent.size() > 0) {
            // bottom out
            list.add(parent);
        }

        while (true) {
            Record child = denormer.next();
            if (child == null) break;

            flatten(child);
        }
    }

    @Override
    public Record next() throws IOException {
        if (list.isEmpty()) return null;
        return list.remove(0);
    }

    @Description(text = {"A source that reveals the origin of a format source (i.e. the sink expression that produced a nv2,tsv,nat,natdb or json source)"})
    @Arg(name = "PATH", gloss = "Path to the source directory.", type = ArgType.STRING)
    @Param(name = "flatten", gloss = "if true, flatten the hierarchical report.", type = ArgType.BOOLEAN, defaultValue = "false")
    @Example(expr = "origin:mysource.nat devnull", type = ExampleType.DISPLAY_ONLY)
    public static class Op extends SourceOperator {

        public Op() {
            super(FORMAT + ":PATH", Type.USAGE);
        }

        @Override
        public RecordSource getSource(OpArgs args) throws IOException {
            return new FormatOriginSource(args);
        }
    }
}
