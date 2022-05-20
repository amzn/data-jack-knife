package com.amazon.djk.pipe;

import java.io.IOException;

import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.core.NestedExpressionPipe;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.Value;
import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.report.PercentProgress;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.source.ElseReceiver;
import com.amazon.djk.source.QueueSource;
import com.amazon.djk.source.ReceiverSource;

/**
 * This class handles 'if' and 'ifNot'.  Subclassing is too complicated.  The
 */
@ReportFormats(headerFormat="<conditional>%s", lineFormats={
        "true=%,d (<percentTrue>%2.1f%%) false=%,d (<percentFalse>%2.1f%%) nonBoolean=%,d"
})
public class IfPipe extends RecordPipe implements NestedExpressionPipe, Keyword {
	public static final String NAME = "if"; 

    private final OpArgs args;
    private final RecordPipe trueClause;
    private final RecordPipe falseClause;
    private RecordPipe activeClause;
    
    @ScalarProgress(name="conditional")
    private final Value conditionalValue;
    private QueueSource conditionalOperand;
    
    @ScalarProgress(name="nonBoolean")
    protected volatile long numNonBools = 0;
    
    @ScalarProgress(name = "inRecs")
    private volatile long inRecs = 0;

    @PercentProgress(name = "percentTrue", denominatorAnnotation = "inRecs")
    @ScalarProgress(name = "true")
    private volatile long trueRecs = 0;

    @PercentProgress(name = "percentFalse", denominatorAnnotation = "inRecs")
    @ScalarProgress(name = "false")
    private volatile long falseRecs = 0;
    
    private final boolean isIfNot;

    /**
     * 
     * @param grepSpec
     * @param trueClause
     * @throws IOException
     */
    public IfPipe(OpArgs args, RecordPipe trueClause, RecordPipe falseClause, boolean isIfNot) throws IOException {
    	this(null, args, trueClause, falseClause, isIfNot);
    }
    
    /**
     * 
     * @param grepSpec
     * @param trueClause
     * @param falseClause
     * @throws IOException
     */
    public IfPipe(RecordPipe root, OpArgs args, RecordPipe trueClause, RecordPipe falseClause, boolean isIfNot) throws IOException {
        super(root);
        this.trueClause = trueClause;
        this.falseClause = falseClause;

        this.isIfNot = isIfNot;
        this.conditionalValue = (Value)args.getArg("CONDITIONAL");
        this.args = args;
        this.conditionalOperand = new QueueSource();
        
        ReceiverSource trueReceiver = ReceiverSource.getReceiver(trueClause);
        trueReceiver.suppressReport(true);
        trueReceiver.addSource(conditionalOperand);
        
        if (falseClause != null) {
        	ReceiverSource falseReceiver = ReceiverSource.getReceiver(falseClause);
        	falseReceiver.suppressReport(true);
            falseReceiver.addSource(conditionalOperand);
        }
    }
    
    @Override
    public Object replicate() throws IOException {
       return localReplicate(false);
    }
    
    @Override
    public Object subReplicate() throws IOException {
       return localReplicate(true);
    }
    
    private Object localReplicate(boolean isSubExpression) throws IOException {
        RecordPipe left = (RecordPipe) trueClause.getStrand(isSubExpression);
        if (left == null) return null;
        
        if (falseClause == null) {
            return new IfPipe(this, args, left, null, isIfNot);
        }

        // right exists, it must replicate too if we are to replicate
        RecordPipe right = (RecordPipe) falseClause.getStrand(isSubExpression);
        if (right == null) return null;
        
        return new IfPipe(this, args, left, right, isIfNot);
    }
    
    @Override
    public NodeReport getReport() {
        if (report == null) {
            report = super.getReport(isIfNot ? "ifNot" : "if");
            
            report.addChildReport(trueClause.getReport());
            if (falseClause != null) {
                report.addChildReport(falseClause.getReport());
            }
        }

        return report;
    }
    
    @Override
    public Record next() throws IOException {

        while (true) {
            // return results if available
            Record record = null;
            if (activeClause != null) {
                record = activeClause.next();
                if (record != null) {
                    return record;
                }
                //we've exhausted operand
            }

            record = super.next();
            if (record == null)
                return null;

            inRecs++;

            // CONDITIONAL that evaluate non-boolean always map to false
            
            // evaluate the condition
            boolean evaluatedConditional = false;
            try {
                Object obj = conditionalValue.getValue(record);
                if (obj != null && (obj instanceof Boolean)) {
                    evaluatedConditional = (Boolean) obj;                    
                }
                
                else {
                    numNonBools++;
                    evaluatedConditional = false;
                }
            }

            // allow for non-existant fields to return false
            catch (NullPointerException e) {
                numNonBools++;
                evaluatedConditional = false;
            }

            if (evaluatedConditional != isIfNot) {
                trueRecs++;
                activeClause = trueClause;
            } else {
                falseRecs++;
                if (falseClause == null) {
                    // no else, return the original record
                    return record;
                }
                activeClause = falseClause;
            }
            // set the conditional operand which the trueClause and falseClause receivers are connected to
            conditionalOperand.add(record, false);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        trueClause.close();
        if (falseClause != null) {
            falseClause.close();
        }
    }
    
    @Override
    public boolean reset() {
        trueClause.doReset();
        if (falseClause != null) {
            falseClause.doReset();
        }
    	return true;
    }

    public RecordPipe getTrueClause(){
        return trueClause;
    }
    
    public RecordPipe getFalseClause(){
        return falseClause;
    }

    @Description(
    	     text={"Provides if-then-else conditional execution of sub-expressions based on a CONDITIONAL."},
    	     contexts={"[ TRUE_EXP if:CONDITIONAL", "[ TRUE_EXP else FALSE_EXP if:CONDITIONAL"})
    @Example(expr="blanks:5 add:val:'{new Random().nextDouble();}' [ add:size:small if:'{d.val < 0.5;}'", type=ExampleType.EXECUTABLE)
    @Example(expr="blanks:5 add:val:'{new Random().nextDouble();}' [ add:size:small else add:size:big if:'{d.val < 0.5;}'", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1 id:2 color:blue ] [ add:foo:bar if:'{l.id == 2;}'", type=ExampleType.EXECUTABLE_GRAPHED)
    public static class Op extends BaseIfOp {
    	
    }
    
    @Gloss(entry="[", def="delimiter indicating the leftmost scope of the predicate")
    @Gloss(entry="TRUE_EXP", def="the if-true expression")
    @Gloss(entry="FALSE_EXP", def="the if-false expression")
    @Gloss(entry="else", def="optional delimiter indicating the leftmost scope of the else clause")
    @Arg(name="CONDITIONAL", gloss="a VALUE evaluating to a boolean or false in the case of non-existant or nonBoolean.", type=ArgType.VALUE)
    public static class BaseIfOp extends PipeOperator {
        private final boolean isIfNot;
    	public BaseIfOp() {
    		super("if:CONDITIONAL");
            isIfNot = false;
    	}
    	
    	public BaseIfOp(boolean isIfNot) {
    	    super("ifNot:CONDITIONAL");
            this.isIfNot = isIfNot;
    	}

        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
			if (operands.size() < 2) {
                throw new SyntaxError("syntax error: requires at least 2 arguments for 'if' operator");
            }

            RecordPipe operand = getAsRecordPipe(operands.pop());
            RecordPipe pipe = null;
            
            pipe = (ReceiverSource.getReceiver(operand) instanceof ElseReceiver) ?
                    new IfPipe(args, getAsRecordPipe(operands.pop()), operand, isIfNot) : // if-else
            		new IfPipe(args, operand, null, isIfNot); // plain if
            
            // DATA
            pipe.addSource(operands.pop());
            return pipe;
		}
		
        private RecordPipe getAsRecordPipe(RecordSource source) throws SyntaxError {
            if (! (source instanceof RecordPipe) ) {
                throw new SyntaxError("improper if-else syntax");
            }
            
            return (RecordPipe) source;
        }
    }
}
