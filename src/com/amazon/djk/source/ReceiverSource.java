package com.amazon.djk.source;

import java.io.IOException;

import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Record;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.core.Splittable;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.report.ProgressData;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.ProgressReportFactory;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

/**
 * Pipe that allows for dynamic setting of input source
 * 
 */
@ReportFormats(headerFormat="<header>%s", lineFormats={
        "<info>%s"
})
public class ReceiverSource implements RecordSource, Splittable {
	public final static String LEFT_SCOPE = "[";
	protected NodeReport report = null;
	protected ProgressData progData = null;
    @ScalarProgress(name="header")
    private String displayString;
    @ScalarProgress(name="info")
    private String info = null;
    private RecordSource transmitter;
    private boolean isSuppressed;
    
    
    public ReceiverSource() {
    	displayString = NodeReport.getDefaultLabel(this);
    }
    
    public ReceiverSource(String displayString) {
    	this.displayString = displayString;
    }
    
    /**
     * 
     * @param reportName for the report.  defaults to class.
     * 
     */
    public void setReportTitle(String title) {
    	this.displayString = title;
    }
    
    public void setInfo(String info) {
    	this.info = info;
    }
    
    @Override
    public Object split() {
        return new ReceiverSource(displayString);
    }
    
    @Override
	public void close() throws IOException {
		//Do nothing because the subexpression pipe closes the source
	}

	/**
     * allows for a pipe to be inserted immediately downstream of the receiver 
     * 
     * @param pipe
     */
    public static RecordPipe getReceiverConsumer(RecordPipe expression) {
        while (true) {
            RecordSource candidate = expression.getSource(); 
            
            if (candidate == null) {
                throw new RuntimeException("improper syntax");                         
            }
            
            else if (candidate instanceof ReceiverSource) {
                return expression;
            }
            
            else if (! (candidate instanceof RecordPipe)) {
                throw new RuntimeException("improper syntax");                         
            }

            expression = (RecordPipe) candidate;
        }        
    }
    
    /**
     * Used in LockStepDiffPipe and SubExpressionPipe in snippet expressions like this:
     * [ pipe1 pipe2 pipe3 ...
     * 
     * Descends the input until hitting the ReceiverPipe
     * (or ReceiverPipe if already replicated) replacing it with the ReceiverPipe. 
     * 
     * @param listSource
     * @return
     * @throws SyntaxError 
     */
    public static ReceiverSource getReceiver(RecordPipe expression) {
        while (true) {
            RecordSource candidate = expression.getSource(); 
            
            if (candidate == null) {
                throw new RuntimeException("improper syntax");                         
            }
            
            else if (candidate instanceof ReceiverSource) {
                return (ReceiverSource) candidate;
            }
            
            else if (! (candidate instanceof RecordPipe)) {
                throw new RuntimeException("improper syntax");                         
            }

            expression = (RecordPipe) candidate;
        }
    }
    
    public void addSource(RecordSource source) {
    	this.transmitter = source;
    }

    @Override
	public void suppressReport(boolean value) {
    	isSuppressed = value;
    } 

	@Override
	public boolean isReportSuppressed() {
		return isSuppressed;
	}

	@Override
	public NodeReport getReport() {
		if (report != null) return report;
    	report =  ProgressReportFactory.create(this, displayString);
    	return report;
	}

	@Override
	public Record next() throws IOException {
		return transmitter.next();
	}
	
	@Description(
            text={"The left scope delimiter. See 'if', 'foreach'.  Also used to define inline records, see ']'."},
            contexts={"[ TRUE_EXP if:CONDITIONAL", "[ REC_SPEC ... ]"})
    public static class Op extends SourceOperator {
    	public Op() {
			super(LEFT_SCOPE, Type.NAME);
		}

    	@Override
    	public RecordSource getSource(OpArgs args) throws IOException, SyntaxError {
    		return new ReceiverSource();
    	}
    }

	@Override
	public ProgressData getProgressData() {
		return new ProgressData(this);
	} 
}
