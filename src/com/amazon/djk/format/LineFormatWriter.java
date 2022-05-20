package com.amazon.djk.format;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Record;

public class LineFormatWriter extends FormatWriter {
	private final PrintWriter writer;
	private final FieldIterator fiter = new FieldIterator();
	
	public LineFormatWriter(File dataFile) throws IOException {
		super(dataFile);
		writer = new PrintWriter(getStream());
	}
	
	@Override
	public void writeRecord(Record rec) throws IOException {
		fiter.init(rec);
		
		while(fiter.next()) {
            String val = fiter.getValueAsString();
            writer.print(val);
            writer.print(" ");
        }

		writer.println();
	}
	
	@Override
	public void close() {
		writer.close();
	}
	
	
    @Description(text={"writes all record fields as strings to a single line separated by a space.  Field names and boundaries are lost."})
	public static class Op extends WriterOperator {
		public Op() {
			super("txt", LineFormatParser.STREAM_FILE_REGEX);
		}

		@Override
		public FormatWriter getWriter(FormatArgs args, File dataFile) throws IOException {
			return new LineFormatWriter(dataFile);
		}
	}
}
