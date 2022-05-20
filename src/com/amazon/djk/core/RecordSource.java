package com.amazon.djk.core;

import java.io.IOException;

import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportProvider;

/**
 * 
 * 
 */
public interface RecordSource extends ReportProvider {
    /**
     * 
     * @return
     * @throws IOException
     */
    Record next() throws IOException;

    /**
     * @throws IOException 
	 * 
	 */
    void close() throws IOException;
    
    public static RecordSource singleton(Record record) throws IOException {
		return new MinimalRecordSource.SingletonSource(record);
	}
}
