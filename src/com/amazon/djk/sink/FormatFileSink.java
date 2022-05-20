package com.amazon.djk.sink;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazon.djk.format.FormatWriter;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;

/**
 * 
 *
 */
@ReportFormats2(headerFormat="file://<path>%s?<params>%s")
public class FormatFileSink extends FileSink {
    private final FormatFileSinkHelper finfo;
    protected final FormatWriter writer;
    
    @ScalarProgress(name="path")
    private final String path; // for display only
    
    @ScalarProgress(name="params")
    protected final String params;
    
    /**
     * main constructor
     * 
     * @param root
     * @param path
     * @throws IOException 
     */
    public FormatFileSink(FormatFileSink root, FormatFileSinkHelper finfo) throws IOException {
        super(root, finfo);
        this.finfo = finfo;
        this.path = finfo.absolutePath();
        this.params = finfo.getArgs().getParamsAsString();
        this.writer = finfo.getWriter(dataFile);
    }
    
    @Override
    public void drain(AtomicBoolean forceDone) throws IOException {
    	super.drain(forceDone);
    	
        try {
        	while (!forceDone.get()) {
        		Record rec = super.next();
        		if (rec == null) break;
            
        		writer.writeRecord(rec);
        		reportSunkRecord(1);
        	}
        }
        
        finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
    
    /**
     * 
     * @return the absolute path of the sink
     */
    public String getAbsolutePath() {
    	return finfo.absolutePath();
    }
    
	@Override
	public String getStreamFileRegex() {
		return finfo.getStreamFileRegex();
	}
}
