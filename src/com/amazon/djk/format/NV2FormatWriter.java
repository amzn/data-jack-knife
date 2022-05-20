package com.amazon.djk.format;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Record;

public class NV2FormatWriter extends FormatWriter {
	private final PrintWriter writer;
	
	public NV2FormatWriter(File dataFile) throws IOException {
		super(dataFile);
		writer = new PrintWriter(getStream());
	}
	
	@Override
	public void writeRecord(Record rec) throws IOException {
        writer.print(rec.getAsNV2(false));
        writer.println("#"); // end of record marker		
	}
	
	@Override
	public void close() throws IOException {
		writer.close();
		//super.close();
	}
	
	
	@Description(text={"Writes records as nv2 file(s)."})
	public static class Op extends WriterOperator {
		public Op() {
			super("nv2", NV2FormatParser.STREAM_FILE_REGEX);
		}

		@Override
		public FormatWriter getWriter(FormatArgs args, File dataFile) throws IOException {
			return new NV2FormatWriter(dataFile);
		}
	}
}
