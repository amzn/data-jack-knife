package com.amazon.djk.pipe;

import java.io.IOException;

import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.keyed.KeyedSource;
import com.amazon.djk.keyed.OuterKeyedSource;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.processor.WithKeyedSource;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.KeyMaker;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.SlowComparableRecord;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.PercentProgress;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;

@ReportFormats(headerFormat="<how>%s", lineFormats={
        "(<hitsPercentLeft>%2.1f%% of left) rightHits=%,d (<hitsPercentRight>%2.1f%% of right)"
})
public class JoinPipe extends RecordPipe implements WithKeyedSource, Keyword {
    public final static String NAME = "join";
    
    public enum JoinHow { 
    	LEFT_OUTER("left"), 
    	RIGHT_OUTER("right"), 
    	INNER("inner"),
        OUTER("outer"),
    	POS_FILTER("+"),
    	POS_DIFF_FILTER("+diff"),
    	NEG_FILTER("-"),
    	ONCE_FILTER("once"),
    	ADD_BOOLEAN("add-boolean");
    
    	private final String stringForm;
    	
    	private JoinHow(String stringForm) {
    		this.stringForm = stringForm;
    	}
    	
    	public static JoinHow get(String stringForm) {
    		for (JoinHow how : JoinHow.values()) {
    			if (stringForm.equals(how.stringForm)) {
    				return how;
    			}
    		}
    		
    		return ADD_BOOLEAN;
    	}

    	@Override
    	public String toString() {
    		return stringForm;
    	}
    	
    	public boolean isFilter() {
    		return (this == POS_FILTER || this == NEG_FILTER || this == ONCE_FILTER || this == POS_DIFF_FILTER);
    	}
    }

    @ScalarProgress(name="how")
    private final String howDisplay;
    private final String howString;
    private final JoinHow how;
    private final KeyMaker keyMaker;
    private final KeyedSource right;
    private final Record diffRec = new Record();

    @ScalarProgress(name="leftRecs")
    private volatile long numLeftRecs = 0;
    
    @PercentProgress(name="hitsPercentLeft", denominatorAnnotation = "leftRecs")
    @ScalarProgress(name="rightHits")
    private volatile long numRightHits = 0;
    
    // can't have two PercentProgress annotations on one field
    @PercentProgress(name="hitsPercentRight", denominatorAnnotation = "rightRecs")
    private volatile long numRightHitsCopy = 0;
    
    @ScalarProgress(name="rightRecs", aggregate=AggType.NONE)
    private final long numRightRecs;

    public JoinPipe(KeyedSource right, String howString) throws IOException {
        this(null, right, howString);
    }
    
    public JoinPipe(RecordPipe root, KeyedSource right, String howString) throws IOException {
        super(root);
        this.howString = howString;
        this.how = JoinHow.get(howString);
        this.howDisplay = how == JoinHow.ADD_BOOLEAN ? "add="+howString : howString;
        this.right = right;
        this.numRightRecs = right.getNumRecords();
        keyMaker = new KeyMaker(right.getKeyFieldNames());
    }
	    
    @Override
    public NodeReport getReport() {
    	if (report != null) return report;
        report = getReport(how.isFilter() ? "Filter" : "Join");
        report.addChildReport( right.getReport() );
        return report;
    }
	    
    @Override 
    public Object replicate() throws IOException {
        KeyedSource rt = (KeyedSource) right.replicateKeyed();
        if (rt == null) return null;
        return new JoinPipe(this, rt, howString);
    }
    
    @Override
    public RecordSource getKeyedSource() {
        return right;
    }

    @Override
    public void close() throws IOException {
        super.close();
        right.close();
    }

    @Override
    public Record next() throws IOException {
        while (true) {
            Record leftRec = super.next();  // returns 'left' records
            if (leftRec == null) return null;
	            
            numLeftRecs++;
            Record rightRec = right.getValue(leftRec);
            
            if (rightRec != null) {
                numRightHits++;
                numRightHitsCopy++;

                switch (how) {
                case LEFT_OUTER: 
                case INNER:
                    // add right other fields to left
                    leftRec.addFields(rightRec);
                    return leftRec;
                    
                case NEG_FILTER:
                    continue;
                    
                case POS_FILTER:
                    return leftRec;
                    
                case POS_DIFF_FILTER:
                	diffRec.reset();
                	keyMaker.copyTo(leftRec, diffRec);
                	keyMaker.removeFrom(leftRec);
                	if (!SlowComparableRecord.diff(diffRec, leftRec, rightRec)) {
                		continue; // identical records
                	}
                	
                	return diffRec;
                    
                case ADD_BOOLEAN:
                	leftRec.addField(howString, true);
                	return leftRec;
                	
                default:
                	// continue
                }
            }
	        
            else { // rightRec == null
                switch (how) {
                case INNER:
                case POS_FILTER:
                    continue;

                case NEG_FILTER:
                case LEFT_OUTER:
                    return leftRec;
                
                case ADD_BOOLEAN:
                	leftRec.addField(howString, false);
                	return leftRec;
                	
                default:
                	// continue
                }
            }
        }
	}

    @Description(text={"Joins the LEFT_SRC with the keyed RIGHT_SRC acccording to HOW.",
    		"The keyed right source defines which fields comprise the KEY."},
    		contexts={"LEFT_SRC RIGHT_SRC join:HOW"})
    @Gloss(entry="left", def="a left outer join (the decoration case)")
    @Gloss(entry="right", def="a right outer join.  Unmatched right records come last.")
    @Gloss(entry="inner", def="an inner join")
    @Gloss(entry="outer", def="full outer join")
    @Gloss(entry="FIELD", def="a boolean valued field to be added to the left record reflecting whether there was a right hit.")
    
    @Example(expr="[ id:1,tx:0 ] [ id:1,color:red id:2,color:blue ] map:id join:left", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1,tx:0 ] [ id:1,color:red id:2,color:blue ] map:id join:right", type=ExampleType.EXECUTABLE)    
    @Example(expr="[ id:1,tx:0 id:2,tx:1 ] [ id:1,color:red id:2,color:blue ] map:id join:inner", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1,tx:0 id:2,tx:1 ] [ id:1,color:red id:3,color:pink ] map:id join:outer", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1,tx:0 id:2,tx:1 ] [ id:1 ] map:id join:keyExists", type=ExampleType.EXECUTABLE)

    @Arg(name="HOW", gloss="left | right | inner | outer | FIELD", type = ArgType.STRING, eg = "left")
    public static class Op extends PipeOperator {
    	public Op() {
    		super(NAME + ":HOW");
        }
    	
    	public Op(String usage) {
    		super(usage);
    	}

        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
        	return getAsPipe(operands, args, false);
        }
    	
        RecordPipe getAsPipe(ParserOperands operands, OpArgs args, boolean asFilter) throws IOException, SyntaxError {
            if (operands.size() < 2) {
                throw new SyntaxError("syntax error: too few input arguments for 'join' operator");           
            }
    		
    		String howString = (String)args.getArg("HOW");
    		JoinHow how = JoinHow.get(howString);
    		
    		if (asFilter != how.isFilter()) {
    			throw new SyntaxError("improper argument");
    		}
    		
    		KeyedSource right = operands.popAsKeyedSource();
    		RecordSource left = operands.pop();
    		
    		Fields keyFields = right.getKeyFields();
    		if (keyFields.isAllFields()) {
    			throw new SyntaxError("right source keyed on all fields is illegal. Field names must be explicit.");
    		}

    		if (how == JoinHow.RIGHT_OUTER || how == JoinHow.OUTER || how == JoinHow.ONCE_FILTER) {
    			if (! (right instanceof OuterKeyedSource) ) {
    	    		throw new SyntaxError("right source must be an OuterKeyedSource");
    			}
    			
    			OuterKeyedSource outerAccessible = (OuterKeyedSource)right;
    			outerAccessible.enableOuterAccess();
    			RecordPipe joiner =	new JoinOuterPipe(outerAccessible, howString); 
    					
				joiner.addSource(left);
				return joiner;
    		}
    		
    		if (right instanceof KeyedSource) {
    			KeyedSource ksource = (KeyedSource) right;
    			RecordPipe joiner =	new JoinPipe(ksource, howString);
    			joiner.addSource(left);
    			return joiner;
    		}

    		throw new SyntaxError("non-keyed right source: '" + right);
        }
    }
}
