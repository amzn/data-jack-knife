package com.amazon.djk.format;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;
import com.amazon.djk.record.RecordIO;

public class NativeFormatWriter extends FormatWriter {
	private final DataOutputStream outstream;
	public final static int FIFO_BUFFER_SIZE = 512 * 1024;
	private final RecordFIFO fifo = new RecordFIFO();

	
	public NativeFormatWriter(File dataFile) throws IOException {
		super(dataFile);
		this.outstream = new DataOutputStream(getStream());
	}
	
	@Override
	public void writeRecord(Record rec) throws IOException {
		fifo.add(rec);
		if (fifo.byteSize() > FIFO_BUFFER_SIZE) {
            RecordIO.write(outstream, fifo);
		    fifo.reset();
		}
	}
	
	@Override
	public void close() throws IOException {
		if (fifo.byteSize() > 0) { // remnant
            RecordIO.write(outstream, fifo);
	    }

		outstream.close();
		//super.close();
	}
	
	
    @Description(text={"writes djk native files within a directory."})
	public static class Op extends WriterOperator {
		public Op() {
			super("nat", NativeFormatParser.STREAM_FILE_REGEX);
		}

		@Override
		public FormatWriter getWriter(FormatArgs args, File dataFile) throws IOException {
			return new NativeFormatWriter(dataFile);
		}
	}
}
