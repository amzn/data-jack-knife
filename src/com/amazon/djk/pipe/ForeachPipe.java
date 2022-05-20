package com.amazon.djk.pipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazon.djk.core.Denormalizer.AddMode;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.core.Denormalizer;
import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.NestedExpressionPipe;
import com.amazon.djk.core.Normalizer;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordIO.IORecord;
import com.amazon.djk.reducer.Reducer;
import com.amazon.djk.reducer.Reductions;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.source.ReceiverSource;

/**
 * This pipe makes it possible to run an expression over the sub-records of
 * its incoming record stream.  Functionally this is equivalent to denormalizing
 * the stream, running the expression and renormalizing.
 * <p>
 * TODO: make apply to regular fields too? all pipes of the expression would
 * need to be required to be reducers since you can't add fields to a field...
 */
@ReportFormats(headerFormat = "<child>%s", lineFormats = {"count=%,d"})
public class ForeachPipe extends RecordPipe implements NestedExpressionPipe, Keyword {
    private final OpArgs args;
    public final static String NAME = "foreach";

    @ScalarProgress(name = "child")
    private final Field child;
    private final RecordPipe subExpr;
    private final Denormalizer denormer;
    private final Normalizer normer;
    private final IORecord outrec = new IORecord();
    private final ReceiverSource receiver;
    private final List<Reducer> reducers;

    @ScalarProgress(name = "count")
    private long count = 0;

    public ForeachPipe(OpArgs args, RecordPipe subPipe) throws IOException {
        this(null, args, subPipe);
    }

    public ForeachPipe(RecordPipe root, OpArgs args, RecordPipe subPipe) throws IOException {
        super(root);
        this.args = args;
        child = (Field) args.getArg("CHILD");

        denormer = new Denormalizer(child.getName(), AddMode.INCLUDE_REMNANT_FIELDS, Denormalizer.Context.SUBEXP);
        normer = new Normalizer(child.getName());

        // grab the sourcing end of the expression, and insert the injector
        receiver = ReceiverSource.getReceiver(subPipe);
        receiver.suppressReport(true);

        // Receiver --> Consumer ... 
        // where Consumer is the first pipe of the subExpression

        this.subExpr = subPipe;

        // subexpression ready to go, collect reducers if any
        reducers = new ArrayList<>();
        Reductions.collectChildReducers(subExpr, reducers);
    }

    public void validate() throws SyntaxError {
        if (!subExpr.reset()) { // call to see if we're legit
            throw new SyntaxError("SubExpression contains non-resetable predicate");
        }
    }

    /**
     * used by StatsTeeIterator to find statsTees in subExpressions
     *
     * @return subExpression of this pipe
     */
    public RecordPipe getSubExpression() {
        return subExpr;
    }

    @Override
    public Object replicate() throws IOException {
        Object strand = subExpr.getStrand(true);
        if (strand == null) {
            throw new RuntimeException("There exists a predicate in the subExpression which does not subReplicate.");
        }

        return new ForeachPipe(args, (RecordPipe) strand);
    }

    @Override
    public NodeReport getReport() {
        if (report == null) { // only first time is null
            NodeReport subReport = subExpr.getReport();
            NodeReport thisReport = super.getReport();
            thisReport.addChildReport(subReport);
            report = thisReport;
            return thisReport;
        }

        return report;
    }

    @Override
    public void close() throws IOException {
        subExpr.close();
        super.close();
    }

    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;
        int inRecSize = rec.size();

        Record remnant = denormer.init(rec); // prepare for denormalizing
        receiver.addSource(denormer); // sends the children into the subPipe here

        /**
         * adding subrecords can be excessively slow do to resizing.  try to improve 
         */
        outrec.reset();
        outrec.resize(inRecSize);

        outrec.addFields(remnant);
        normer.normalize(outrec, subExpr); // out the subPipe into the normer here
        count += normer.getNumNormalized();

        // reduce into the parent
        for (Reducer reducer : reducers) {
            Record rx = reducer.getChildReduction();
            if (rx == null) continue;
            outrec.addFields(rx);
        }

        subExpr.doReset(); // reset for the next record

        return outrec;
    }

    @Description(text = {"Executes EXP for each CHILD, whether sub-record or field. The remaining fields of the record 'appear' to be members",
            "of the child records during execution. When CHILD is a field, the results of the EXP expression are added directly",
            "to the parent for each field instance. In this latter case, it makes most sense for an expression to create a field",
            "which is a function of the CHILD field value or contain a reducer."},
            contexts = {"[ EXP foreach:word"})
    @Arg(name = "CHILD", gloss = "the name of the sub-record or field", type = ArgType.FIELD, eg = "word")
    @Gloss(entry = "EXP", def = "expression to be executed for each CHILD.")
    @Example(expr = "[ id:1,word:[text:cars],word:[text:trees],word:[text:door] ] [ add:isPlural:'{s.text.endsWith(\"s\");}' foreach:word", type = ExampleType.EXECUTABLE_GRAPHED)
    @Example(expr = "[ foo:bar,id:1,id:2,id:3 ] [ numIds=sum:1 foreach:id", type = ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
        public Op() {
            super("foreach:CHILD");
        }

        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            RecordSource temp = operands.pop();
            if (!(temp instanceof RecordPipe)) {
                throw new SyntaxError("expected '[ EXP' immediately to the left of 'foreach', where EXP is one or more pipes.");
            }
            RecordPipe subExp = (RecordPipe) temp;

            ForeachPipe subPipe = new ForeachPipe(args, subExp);
            subPipe.validate();

            return subPipe.addSource(operands.pop());
        }
    }
}
