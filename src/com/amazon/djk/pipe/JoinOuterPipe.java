package com.amazon.djk.pipe;

import java.io.IOException;

import com.amazon.djk.report.PercentProgress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.keyed.OuterKeyedSource;
import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.pipe.JoinPipe.JoinHow;
import com.amazon.djk.processor.WithKeyedSource;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.KeyMaker;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;

@ReportFormats2(headerFormat="<how>%s",
	lineFormats={
        "(<hitsPercentLeft>%2.1f%% of left) rightHits=%,d (<hitsPercentRight>%2.1f%% of right)"
})
public class JoinOuterPipe extends RecordPipe implements WithKeyedSource, Keyword {
    private static final Logger logger = LoggerFactory.getLogger(JoinOuterPipe.class);
    private final OuterKeyedSource rtKeyed;

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
    
    @ScalarProgress(name="how")
    private final String howString;
    private final JoinHow how;
    
    private boolean leftExhausted = false;

    /**
     * 
     * @param right
     * @throws IOException
     */
    public JoinOuterPipe(OuterKeyedSource right, String howString) throws IOException {
        this(null, right, howString);
    }

    public JoinOuterPipe(RecordPipe root, OuterKeyedSource right, String howString) throws IOException {
        super(root);
        this.rtKeyed = right;
        this.howString = howString;
        how = JoinHow.get(howString);
        numRightRecs = right.getNumRecords();
    }

    @Override
    public NodeReport getReport() {
        if (report != null) return report;
        report = getReport(how.isFilter() ? "Filter" : "Join");
        report.addChildReport( rtKeyed.getReport() );
        return report;
    }

    @Override
    public Object replicate() throws IOException {
    	OuterKeyedSource mapSource = (OuterKeyedSource)rtKeyed.replicateKeyed();
        return new JoinOuterPipe(this, mapSource, howString);
    }

    @Override
    public Object subReplicate() throws IOException {
        throw new RuntimeException("right join invalid subExpression context");
    }

    @Override
    public void close() throws IOException {
        super.close();
        rtKeyed.close();
    }

    @Override
    public RecordSource getKeyedSource() {
        return rtKeyed;
    }

    @Override
    public Record next() throws IOException {
        while (!leftExhausted) {
            Record leftRec = super.next();
            if (leftRec == null) {
                leftExhausted = true;

                if (isLastSync1.arriveAndIsLast()) {
                    // make ready for nexting
                    rtKeyed.prepareOuterAccess();
                }

                return next(); // RECURSE ONCE
            }

            numLeftRecs++;
            Record rightRec = how == JoinHow.ONCE_FILTER ?
            		rtKeyed.getValueOnce(leftRec) : rtKeyed.getValue(leftRec);

            if (rightRec != null) {
            	numRightHits++;
            	numRightHitsCopy++;
            	
            	if (how == JoinHow.ONCE_FILTER) {
            		return leftRec;
            	}
            	
                // case of RIGHT_OUTER and OUTER
                leftRec.addFields(rightRec);
                return leftRec;
            } 

            // case of OUTER
            else if (how == JoinHow.OUTER) {
                return leftRec;
            }
        }
        
        return how == JoinHow.ONCE_FILTER ? 
        		null : // we're filtering left and we're exhausted 
        		rtKeyed.next(); // return non-joined rt records for OUTER and RIGHT
    }
}
