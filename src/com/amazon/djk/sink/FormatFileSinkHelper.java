package com.amazon.djk.sink;

import java.io.File;
import java.io.IOException;

import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.format.FormatWriter;
import com.amazon.djk.format.WriterOperator;

public class FormatFileSinkHelper extends FileSinkHelper {
	private final WriterOperator writeOp;
	private final FormatArgs fargs;

	/**
	 * constructor for format
	 * 
	 * @param args
	 * @throws IOException
	 */
	public FormatFileSinkHelper(FormatArgs fargs, WriterOperator writeOp) throws IOException {
		super(fargs.getPath(),
			 fargs.getFormat(),
			 (Boolean)fargs.getParam("overwrite", false),
			 (Boolean)fargs.getParam("asFile", false),
			 (Boolean)fargs.getParam("gzip", true));
		this.writeOp = writeOp;
		this.fargs = fargs;
	}

	public FormatWriter getWriter(File dataFile) throws IOException {
		return writeOp.getWriter(fargs, dataFile);
	}

	public String getStreamFileRegex() {
		return writeOp.getStreamFileRegex();
	}
	
	public FormatArgs getArgs() {
		return fargs;
	}
}
