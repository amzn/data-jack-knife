package com.amazon.djk.pipe;

import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.NestedExpressionPipe;
import com.amazon.djk.core.Normalizer;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.reducer.Reducer;
import com.amazon.djk.reducer.Reductions;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.source.QueueSource;
import com.amazon.djk.source.ReceiverSource;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@ReportFormats(headerFormat="<injectFields>%s -> <child>%s")
public class InjectPipe extends RecordPipe implements NestedExpressionPipe,Keyword {
    private static final String DEFAULT_CHILD_NAME = "child";
    private final OpArgs args;
	public final static String NAME = "inject";

    @ScalarProgress(name="child")
    private final Field child;
    
    @ScalarProgress(name="injectFields")
    private final String injectFields;
    
	private final Record injectable = new Record();
    private final RecordPipe expr;
    private final QueueSource queueForOne = new QueueSource(); // need a RecordSource
    private final Normalizer normer;
    private final ReceiverSource receiver;
    private final List<Reducer> reducers;
    private final FieldIterator fiter;
    
    public InjectPipe(OpArgs args, RecordPipe subPipe) throws IOException {
        this(null, args, subPipe);
    }
    
    public InjectPipe(RecordPipe root, OpArgs args, RecordPipe expr) throws IOException {
        super(root);
    	this.args = args;
    	child = (Field)args.getParam("child");
        Fields only = (Fields)args.getParam("only");
        Fields except = (Fields)args.getParam("except");

        injectFields = only != null ? StringUtils.join(only.getFieldNames(), ",") :
            except != null ? "except(" + StringUtils.join(except.getFieldNames(), ",") + ")" :
                "*";
        
        fiter = only != null ? only.getAsIterator() :
            except != null ? except.getAsNotIterator() : null;

        normer = new Normalizer(child.getName());

        // grab the sourcing end of the expression, and insert the injector
        receiver = ReceiverSource.getReceiver(expr);
        receiver.suppressReport(true);
            
        // Receiver --> Consumer ... 
        // where Consumer is the first pipe of the subExpression

        this.expr = expr;

        // subexpression ready to go, collect reducers if any
        reducers = new ArrayList<>();
        Reductions.collectChildReducers(expr, reducers);
    }
    
    public void validate() throws SyntaxError {
        if (!expr.reset()) { // call to see if we're legit
        	throw new SyntaxError("SubExpression contains non-resetable predicate");
        }
    }
    
    /**
     * used by StatsTeeIterator to find statsTees in subExpressions
     * 
     * @return subExpression of this pipe
     */
    public RecordPipe getSubExpression() {
        return expr;
    }
    
    @Override
    public Object replicate() throws IOException {
        Object strand = expr.getStrand(true);
        if (strand == null) {
        	throw new RuntimeException("There exists a predicate in the expression which does not subReplicate.");
        }
        
        return new InjectPipe(args, (RecordPipe)strand);
    }
    
    @Override
    public NodeReport getReport() {
        if (report == null) { // only first time is null
            NodeReport subReport = expr.getReport();
            NodeReport thisReport = super.getReport();
            thisReport.addChildReport(subReport);
            report = thisReport;
            return thisReport;
        }
        
        return report;
    }
    
    @Override
    public void close() throws IOException {
        expr.close();
        super.close();
    }
    
    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;
        
        queueForOne.clear();
        injectable.reset();

        if (fiter == null) {
            injectable.addFields(rec);
        } else {
            fiter.init(rec);
            while (fiter.next()) {
                injectable.addField(fiter);
            }
        }
        queueForOne.add(injectable, false); // no copy
        
        
        receiver.addSource(queueForOne); // sends the record into the exp here
        normer.normalize(rec, expr); // out the exp into the normer here
        
        // reduce into the parent
        for (Reducer reducer : reducers) {
        	Record rx = reducer.getChildReduction();
        	if (rx == null) continue;
        	rec.addFields(rx);
        }
        
        expr.doReset(); // reset for the next record
        
        return rec;
    }

    @Description(text={"injects record into EXP and joins in the resulting source as 'child' sub-records."},
    		contexts={"... [ EXP inject"})
    @Gloss(entry="EXP", def="expression to be executed for each record.")
    @Param(name="child", gloss="name of the child sub-record", type=ArgType.FIELD, defaultValue = DEFAULT_CHILD_NAME)
    @Param(name="only", gloss="comma separated list of the only fields to inject.  Otherwise all fields are injected. Not compatible with 'except' param", type=ArgType.FIELDS)
    @Param(name="except", gloss="comma separated list of fields excluded from injection.  Otherwise all fields are injected. Not compatible with 'only' param", type=ArgType.FIELDS)
    @Example(expr="[ id:1,hello:world ] [ noOp inject", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1,color:blue ] [ noOp inject'?only=color", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("inject");
    	}
            
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		RecordSource temp = operands.pop();
    		if (! (temp instanceof RecordPipe) ) {
    			throw new SyntaxError("expected '[ EXP' immediately to the left of 'inject', where EXP is one or more pipes.");
    		}
    		RecordPipe expr = (RecordPipe) temp;
    		
    		Fields only = (Fields)args.getParam("only");
            Fields except = (Fields)args.getParam("except");
            if (only != null && except != null) {
                throw new SyntaxError("include and exclude parameters not compatible with each other");
            }
    			
    		InjectPipe injector = new InjectPipe(args, expr);
    		injector.validate();
                
    		return injector.addSource(operands.pop());
    	}
    }
}
