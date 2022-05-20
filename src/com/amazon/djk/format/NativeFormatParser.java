package com.amazon.djk.format;

import com.amazon.djk.file.FileQueue.LazyFile;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.RecordFIFO;
import com.amazon.djk.record.RecordIO;

import java.io.DataInputStream;
import java.io.IOException;

public class NativeFormatParser extends FileFormatParser {
    public final static String STREAM_FILE_REGEX = "\\.nat(\\.gz)?$";
	public static final String FORMAT = "nat";
	private final SourceProperties props;
	private final RecordIO recordIO;
    DataInputStream dis = null;
	
	public NativeFormatParser(SourceProperties props) throws IOException {
		this.props = props;
		recordIO = new RecordIO(props.getSourceFields());
	}
	
    @Override
    public boolean fill(RecordFIFO fifo) throws IOException {
        if (dis == null) return false;
        boolean notDone = recordIO.fill(dis, fifo);
        if (!notDone) {
            dis.close();
            dis = null;
        }
        
        return notDone;
    }
	
	@Override
	public Object replicate() throws IOException {
		return new NativeFormatParser(props);
	}
	
    @Override
    public void initialize(LazyFile file) throws IOException {
        dis = new DataInputStream(file.getStream());
    }

	@Description(text={"reads djk native files from a directory."})
	public static class Op extends FormatOperator {
		public Op() {
			super(FORMAT, STREAM_FILE_REGEX);
		}

		@Override
		public FormatParser getParser(SourceProperties props) throws IOException {
			return new NativeFormatParser(props);
		}
	}
}
