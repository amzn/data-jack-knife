package com.amazon.djk.source;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.amazon.djk.core.BaseRecordSource;
import com.amazon.djk.core.Splittable;
import com.amazon.djk.file.FileQueue;
import com.amazon.djk.file.FileQueue.LazyFile;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.format.FileFormatParser;
import com.amazon.djk.format.FormatOperator;
import com.amazon.djk.format.FormatParser;
import com.amazon.djk.format.PushbackLineReader;
import com.amazon.djk.format.ReaderFormatParser;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;
import com.amazon.djk.record.ThreadDefs;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;
/*
 * class for reading format parser sources without enqueuing one per stream
 */
@ReportFormats2(headerFormat="<uri>%s?format=%s" , lineFormats = { "numStreams=%d" })
public class FormatParserSource extends BaseRecordSource implements Splittable {
	private final FormatParser parser;
	private final FileQueue files;
    @ScalarProgress(name="numStreams", aggregate=AggType.NONE)
    private final int numStreams;
    private final SourceProperties props;
	private final RecordFIFO fifo = new RecordFIFO();

	private int numReps = 1;
	private PushbackLineReader reader = null;
    private DataInputStream dataStream = null;
    @ScalarProgress(name="format")
    private final String format;
    @ScalarProgress(name="uri")
    private final String uri;
    private final int numAllowErrors;

	public FormatParserSource(FormatParser parser, FileQueue files, SourceProperties props)
			throws IOException {
		this.parser = parser;
		this.files = files;
		this.props = props;
		this.numStreams = files.initialSize();
		FormatArgs args = props.getAccessArgs();
		numAllowErrors = (int)args.getParam(FormatOperator.ALLOW_ERRORS);
		
		if (props != null) {
		    reportTotalRecords(props.totalRecs());
		}
		
        format = props != null ? props.getSourceFormat() : "";
        uri = props != null ? props.getSourceURI() : "";

	}
	
    @Override
	public Object split() throws IOException {
		if (numReps >= ThreadDefs.get().getNumSinkThreads()) {
			return null;
		}

		FormatParserSource rep = new FormatParserSource(
				(FormatParser) parser.replicate(), files, props);
		numReps++;
		return rep;
	}
	
	@Override
	public Record next() throws IOException {
		return (parser instanceof ReaderFormatParser) ? 
				nextFromReader() : nextFromStream();
	}
	
	public Record nextFromReader() throws IOException {
        ReaderFormatParser rParser = (ReaderFormatParser)parser;
        Record rec = fifo.next();
	    
	    while (rec == null) {
	        
	        while (!rParser.fill(reader, fifo, numAllowErrors)) {
	            if (reader != null) reader.close();
	            
	            LazyFile file = files.next();
	            if (file == null) return null;
	            
	            InputStream is = file.getStream();
	            InputStreamReader isr = new InputStreamReader(is);
	            reader = new PushbackLineReader(isr);
	            rParser.doInitialize(reader);
	        }
	        
	        rec = fifo.next();
	    }
	    
        reportSourcedRecord(rec);

		return rec;
	}
	
	public Record nextFromStream() throws IOException {
	    FileFormatParser fParser = (FileFormatParser)parser;
	    Record rec = fifo.next();
	    
	    while (rec == null) {
	        
            while (!fParser.outerFill(fifo, numAllowErrors)) {
                if (dataStream != null) dataStream.close();
                
                LazyFile file = files.next();
                if (file == null) return null;

                fParser.doInitialize(file);
            }
	        
            rec = fifo.next();
        }
	    
	    reportSourcedRecord(rec);

		return rec;
	}
}
