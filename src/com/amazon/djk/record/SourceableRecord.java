package com.amazon.djk.record;

import java.io.IOException;

import com.amazon.djk.core.RecordSource;
import com.amazon.djk.report.NodeReport;
import com.amazon.djk.report.ProgressData;

/**
 * A record that can be used as a record source with itself its only record. 
 *
 */
public class SourceableRecord extends Record implements RecordSource {
	private volatile boolean isDone = false;

	@Override
	public void reset() {
		isDone = false;
	}

	@Override
	public void close() throws IOException { 
		isDone = true;
	}
	
	@Override
	public Record next() throws IOException {
		Record rec = isDone ? null : this;
		isDone = true;
		return rec;
	}

	@Override
	public void suppressReport(boolean value) { }

	@Override
	public boolean isReportSuppressed() { return true;	}

	@Override
	public ProgressData getProgressData() {
		return null;
	}

	@Override
	public NodeReport getReport() {
		return null;
	}
}
