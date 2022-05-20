package com.amazon.djk.core;

import java.io.IOException;

import com.amazon.djk.processor.WithInnerSink;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ProgressData;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.ProgressReportFactory;

/**
 * 
 * base record source with no reporting
 *
 */
public class MinimalRecordSource implements RecordSource {
	protected NodeReport report = null;
	private boolean suppressReport = false;

	@Override
	public Record next() throws IOException {
		return null;
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public void suppressReport(boolean value) { 
		suppressReport = value;
	} 

	@Override
	public boolean isReportSuppressed() {
		return suppressReport;
	}

	@Override
	public NodeReport getReport() {
		if (report != null) return report;
    	report =  ProgressReportFactory.create(this);
    	
    	// added as child but this is a different "meaning" of child.
    	if (this instanceof WithInnerSink) {
    		RecordSink inner = ((WithInnerSink)this).getSink();
    		if (inner != null) {
        	    report.addChildReport( inner.getReport() );
        	}
    	}
    	
    	return report;
	}
	
	public ProgressData getProgressData() {
		return new ProgressData(this);
	}
	
	public static class SingletonSource extends MinimalRecordSource {
		private final Record rec;
		private boolean done = false;

		public SingletonSource(Record record) throws IOException {
			rec = record.getCopy();
		}
		
		@Override
		public Record next() throws IOException {
			if (done) return null;
			done = true;
			return rec;
		}
	}
}
