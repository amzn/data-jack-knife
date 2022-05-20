package com.amazon.djk.processor;

import java.io.IOException;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.CommaList;
import com.amazon.djk.expression.Expression;
import com.amazon.djk.expression.ExpressionChunks;
import com.amazon.djk.expression.ExpressionParser;
import com.amazon.djk.expression.FieldDeclaration;
import com.amazon.djk.expression.JobProperty;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.processor.KnifeProperties.Namespace;
import com.amazon.djk.record.ThreadDefs;
import com.amazon.djk.reducer.LazyReductionSource;
import com.amazon.djk.reducer.Reductions;
import com.amazon.djk.report.ElapsedTime;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.ProgressData;
import com.amazon.djk.report.ProgressReport;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ReportProvider;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.sink.PrintSink;

/**
 * Takes a JackKnife and an Expression and turns it into a ExecutionContext that can be
 * run by that JackKnife.  It sets and resolves JobProperties and sets FieldDeclarations
 * of the Expression.
 */
@ReportFormats(lineFormats = {
        "expression=%s",
        "host=%s cores=%d sourceThreads=%d sinkThreads=%d",
        "<elapsed>%s date=%s",
}, maxLineLength = 1000)
public class ExecutionContext implements ReportProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);
    private final CommaList DEFAULT_INSTANCE = new CommaList("default");

    @ScalarProgress(name = "expression")
    private final String expressionDisplay;

    private final Expression expression;
    private final RecordSink mainSink;
    private final RecordSource upstream;
    private final ExpressionParser parser;
    private final InnerKnife knife;

    private final Map<CommaList, RecordSink> reduceSinks;

    // DateResolver has a bug where it keeps advancing
    @ScalarProgress(name = "date")
    private final String date;

    @ScalarProgress(name = "host")
    private final String host;

    @ScalarProgress(name = "cores")
    private final int numCores;

    @ScalarProgress(name = "elapsed")
    private final ElapsedTime elapsedTime = new ElapsedTime().start();

    private boolean isReportSuppressed = false;
    protected ProgressReport contextReport = null;

    @ScalarProgress(name = "sourceThreads")
    private final int numSourceThreads;

    @ScalarProgress(name = "sinkThreads")
    private final int numSinkThreads;

    /**
     * @param knife
     * @param exp
     * @return
     * @throws IOException
     */
    public static ExecutionContext create(InnerKnife knife, Expression exp) throws IOException {
        return new ExecutionContext(knife, null, exp);
    }

    /**
     * @param knife
     * @param upstream
     * @param exp
     * @return
     * @throws IOException
     */
    public static ExecutionContext create(InnerKnife knife, RecordSource upstream, Expression exp) throws IOException {
        return new ExecutionContext(knife, upstream, exp);
    }

    /**
     * @param knife
     * @param operand
     * @param expr
     * @throws SyntaxError
     * @throws IOException
     */
    protected ExecutionContext(InnerKnife knife, RecordSource operand, Expression expr) throws SyntaxError, IOException {
        this.knife = knife;
        this.expression = expr;
        this.upstream = operand;
        this.parser = knife.getParser();
        this.expressionDisplay = expr.getAsCommandLine();

        Calendar calendar = Calendar.getInstance();
        //https://docs.oracle.com/javase/7/docs/api/java/util/Formatter.html
        date = String.format("%1$te.%1$tB.%1$tY %1$tT", calendar);
        host = java.net.InetAddress.getLocalHost().getHostName();
        numCores = Runtime.getRuntime().availableProcessors();

        setKnifeProperties(expr); // set System Job Properties
        declareFields(expr);
        setSystemProperties(expr);

        ExpressionChunks chunks = expr.getMainChunks();
        if (chunks != null && !chunks.isEmpty()) {
            this.mainSink = parser.getSink(upstream, chunks);
        } else {
            this.mainSink = null;
        }

        reduceSinks = reduceSinks();
        numSourceThreads = ThreadDefs.get().getNumSourceThreads();
        numSinkThreads = ThreadDefs.get().getNumSinkThreads();
    }

    /**
     * @return true if the Main or any Reduce Expression is using the PrintSink.
     */
    public boolean isUsingPrintSink() {
        if (mainSink instanceof PrintSink) return true;

        Collection<RecordSink> reducerSinks = reduceSinks.values();
        for (RecordSink sink : reducerSinks) {
            if (sink instanceof PrintSink) return true;
        }

        return false;
    }

    public long getMainNumSunk() {
        return mainSink != null ? mainSink.totalRecsSunk() : -1;
    }

    /**
     * @return
     * @throws IOException
     */
    public RecordSink getMainSink() throws IOException {
        return mainSink;
    }

    /**
     * @return the sink for the report expression
     * @throws SyntaxError
     * @throws IOException
     */
    public RecordSink getReportSink() throws SyntaxError, IOException {
        ExpressionChunks reportChunks = expression.getReportChunks();
        if (reportChunks.size() == 0) return null;
        return parser.getSink(contextReport.getAsRecordSource(true), reportChunks);
    }


    public Map<CommaList, RecordSink> getReduceSinks() throws IOException {
        return reduceSinks;
    }

    /**
     * declare fields (STRING,LONG,DOUBLE also 'ephemeral')
     *
     * @throws IOException
     */
    static void declareFields(Expression expr) throws IOException {
        // set fields
        CoreDefs defs = CoreDefs.get();
        for (FieldDeclaration dec : expr.getFieldDeclarations()) {
            if (dec.isEphemeral) {
                defs.defineEphemeralField(dec.field, dec.type, dec.value);
                LOG.info(String.format("ephemeral field declare: %s %s=%s", dec.type, dec.field, dec.value));
            } else {
                defs.declareField(dec.field, dec.type);
                LOG.info(String.format("field declare: %s %s", dec.type, dec.field));
            }
        }
    }

    /**
     * set knife properties that later are resolved within expressions (via ${VAR} syntax).
     *
     * @throws IOException
     */
    static void setKnifeProperties(Expression expr) throws IOException {
        Namespace propertiesNamespace = ThreadDefs.get().getPropertiesNamespace();

        List<JobProperty> properties = expr.getJobProperties();

        // enumerate these
        for (JobProperty prop : properties) {
                // core properties look like knife properties. core props are handled in setSystemProperties
			if (!CoreDefs.isCoreProperty(prop.nonNamespacedName)) {

				String resolvedValue = KnifeProperties.setProperty(propertiesNamespace,
						prop.nonNamespacedName,
						prop.unresolvedValue);
				LOG.info("setting system property namespace=" + propertiesNamespace + " name=" + prop.nonNamespacedName + " value=" + resolvedValue);
					prop.setResolvedValue(resolvedValue);
            }
        }
    }

    /**
     * was used at one time.
     *
     * @throws IOException
     *
    static void resolveKniveProperties(Expression exp) throws IOException {
    Map<String,ExpressionChunks> parts = exp.getParts();
    Namespace propertiesNamespace = ThreadDefs.get().getPropertiesNamespace();
    for (ExpressionChunks chunks : parts.values()) {
    chunks.resolveProperties(propertiesNamespace);
    }
    }*/

    /**
     * @return
     * @throws IOException
     */
    private Map<CommaList, RecordSink> reduceSinks() throws IOException {
        Map<CommaList, ExpressionChunks> reduceMacros = expression.getReduceExpressionChunks();
        Reductions reductions = new Reductions(mainSink);

        /**
         * iterate through the named REDUCE:<instance> expressions, mapping them to reductions and running them.
         * (skip the default expression which is REDUCE ... so it is available for unassigned reductions)
         */
        Set<CommaList> instanceLists = reduceMacros.keySet();
        Map<CommaList, RecordSink> reduceSinks = new HashMap<>();
        for (CommaList instances : instanceLists) {
            if (instances.equals(DEFAULT_INSTANCE)) {
                continue; // leave the default expression to pick up the unassigned
            }

            LazyReductionSource reductionSource = reductions.getAssignedReduction(instances);
            // if no assignable (no 'instance=' on a reducers) we have a problem
            // we could ignore it
            if (reductionSource == null) {
                throw new SyntaxError("no reducer instances assignable to:" + instances);
            }

            ExpressionChunks chunks = reduceMacros.get(instances);
            RecordSink rsink = parser.getSink(reductionSource, chunks);
            reduceSinks.put(instances, rsink);
        }

        /**
         * all REDUCE:<instance> expressions have been assigned, are there unassigned reductions?
         * for the default REDUCE expression to pick up?
         */

        RecordSource reductionSource = reductions.getUnassignedReduction();
        if (reductionSource == null) return reduceSinks;

        // there are unassigned reductions, get the default REDUCE if it exists
        ExpressionChunks chunks = reduceMacros.get(DEFAULT_INSTANCE);
        RecordSink rsink = null;

        if (chunks == null) {
            rsink = new PrintSink(System.out);
            rsink.addSource(reductionSource);
        } else {
            rsink = parser.getSink(reductionSource, chunks);
        }

        reduceSinks.put(new CommaList("default"), rsink);

        return reduceSinks;
    }

    public Expression getExpression() {
        return expression;
    }

    /**
     * sets system properties (num Sink/Source/Sort threads, print precision)
     *
     * @param exp
     * @throws IOException
     */
    private void setSystemProperties(Expression exp) throws IOException {
        CoreDefs cdefs = CoreDefs.get();
        Namespace propertiesNamespace = cdefs.getPropertiesNamespace();

        // set system variables
        for (JobProperty prop : expression.getJobProperties()) {
            boolean propSet = cdefs.setCoreProperty(prop.nonNamespacedName, prop.unresolvedValue);
            if (!propSet) { // not a core property, set namespaced properties
            	String resolvedValue = KnifeProperties.setProperty(propertiesNamespace,
						prop.nonNamespacedName,
						prop.unresolvedValue);
				LOG.info(String.format("setting property %s=%s in namespace=%s", prop.nonNamespacedName, resolvedValue, propertiesNamespace));
				prop.setResolvedValue(resolvedValue);
            }
        }
    }

    @Override
    public ProgressReport getReport() {
        if (contextReport != null) return contextReport; // singleton object
        contextReport = new ProgressReport(this);

        // these need to be added russian-doll wise
        NodeReport otherReports = null;
        for (CommaList list : reduceSinks.keySet()) {
            RecordSink reduceSink = reduceSinks.get(list);

            if (otherReports == null) {
                otherReports = reduceSink.getReport();
            } else {
                otherReports.addLeftReport(reduceSink.getReport());
            }
        }

        /*
         * whether to include the reportSink, this is catch-22y.
         * for now we exclude altogether, we could include it
         * reflecting the pre-execution state. In any case we need
         * some kind of Lazy RecordSource for it.
         */
		/*
		if (reportSink != null) {
			if (otherReports == null) {
				otherReports = reportSink.getReport();
			} else {
				otherReports.addSourceReport(reportSink.getReport());
			}
		}*/

        // this isn't really a SOURCE per se, change the name to
        // addLeftReport or something
        NodeReport mainReport = mainSink.getReport();
        if (otherReports != null) {
            mainReport.addLeftReport(otherReports);
        }

        contextReport.addLeftReport(mainReport);

        return contextReport;
    }

    @Override
    public void suppressReport(boolean value) {
        isReportSuppressed = value;
    }

    @Override
    public boolean isReportSuppressed() {
        return isReportSuppressed;
    }

    @Override
    public ProgressData getProgressData() {
        return new ProgressData(this);
    }
}
