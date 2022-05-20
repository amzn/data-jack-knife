package com.amazon.djk.natdb;

import java.io.IOException;

import com.amazon.djk.file.FileQueue.LazyFile;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.format.FileFormatParser;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;
import com.amazon.djk.record.RecordIO.IORecord;

public class NativeDBFileParser extends FileFormatParser {
    public final static String STREAM_FILE_REGEX = "\\.ndb$";
	private final SourceProperties props;
    private final DiskEntryDecoder decoder;
    
    private final IORecord undecodedRec = new IORecord();
    private final Record outRec = new Record();
    
    private BucketAccess accessor = null;
    
	public NativeDBFileParser(SourceProperties props) throws IOException {
		this.props = props;
		decoder = new DiskEntryDecoder(props);
	}

    @Override
    public void initialize(LazyFile file) throws IOException {
        accessor = BucketAccess.create(file.getLeafArgs());
    }
    
	@Override
	public boolean fill(RecordFIFO fifo) throws IOException {
	    if (accessor == null) return false;
	    fifo.reset();

	    do {
	    	if (!accessor.nextUndecoded(undecodedRec)) {
	    		break;
	    	}
	    	decoder.decode(undecodedRec, outRec, true);
	        fifo.add(outRec);
	    } while (fifo.byteSize() < 1024 * 64);
	        
	    return fifo.byteSize() != 0;
	}
	
	 @Override
	 public Object replicate() throws IOException {
		return new NativeDBFileParser(props);
	}
}