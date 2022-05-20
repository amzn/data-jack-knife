package com.amazon.djk.sink;

import java.io.File;
import java.io.IOException;

import com.amazon.djk.core.RecordSink;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.manual.Display.DisplayType;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.GraphDisplay;
import com.amazon.djk.report.ProgressReport;
import com.amazon.djk.report.ReportConsumer;

/**
 * 
 *
 */
public abstract class FileSink extends RecordSink implements ReportConsumer {
    // the path of this sink
    private final File sinkPath; 
    private final FileSinkHelper info;
    
    // the file containing records for this sink instance
    protected final File dataFile;
    
    /**
     * main constructor
     * @param root
     * @param dataFile
     * @throws IOException 
     */
    public FileSink(FileSink root, FileSinkHelper info) throws IOException {
        super(root);
        this.info = info;
        this.sinkPath = info.getSinkPath();
        this.dataFile = info.getDataFile();
    }

    // NOTE: replicate() implemented anonymously in FormatFactory
    
    public abstract String getStreamFileRegex();
    
    /**
	 * extending classes should call super.close() just before returning
	 * within their Overriding implementation 
	 * 
	 * @throws IOException
	 */
    @Override
    public void close() throws IOException {
        if (!info.asFile() && isLastSync1.arriveAndIsLast()) {
            SourceProperties.write(sinkPath, totalRecsSunk(), info.format(), getStreamFileRegex(), null);
        }
        
        // check if we have a zero length file, in which case delete it.
        if (dataFile.exists() && dataFile.length() == 0) {
        	dataFile.delete();
        }
        
        super.close();
    }
    
    @Override
    public void consume(ProgressReport report) throws IOException {
        if (info.asFile()) return; // no place to write it
        writeReport(report, sinkPath);
    }
    
    public static void writeReport(ProgressReport report, File outputDir) throws IOException {
        // write the report info pretty human readable
        File reportFile = new File(outputDir, "txt.report");
        GraphDisplay display = new GraphDisplay(DisplayType.VT100, false);
        report.display(display);
        display.render(reportFile);
        
        Record originReport = report.getAsRecord(false);
        FileSinkHelper.persistOriginReport(originReport, outputDir);
    }
}
