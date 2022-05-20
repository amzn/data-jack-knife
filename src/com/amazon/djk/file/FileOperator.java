package com.amazon.djk.file;

import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.expression.SyntaxError;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Param(name=FileOperator.FILE_FORMAT_PARAM, gloss = "But mandatory if file extension not used as format hint (e.g. out.tsv).  Specifies the format of the input file." , type=ArgType.STRING)
public abstract class FileOperator extends SourceOperator {
    public final static String PATH_ARG = "PATH";
    public final static String FILE_FORMAT_PARAM = "format";
    public final static String FILE_VALID_REGEX_PARAM = "validRegex";
    private final String format;
	protected final Pattern defaultFormatRegex;
	
	public FileOperator(String format, String defaultFormatRegex) {
		super(format, PATH_ARG);
		this.format = format;
	
		// e.g. "\\.nvp(\\.gz)?$"
		this.defaultFormatRegex = defaultFormatRegex != null ?
					Pattern.compile(defaultFormatRegex) : null;
	}
		
	public String getFormat() {
	    return format;
	}

	public String getStreamFileRegex(){
		return defaultFormatRegex == null ? null : defaultFormatRegex.toString();
	}
	/**
	 * 
	 * @param path path for testing if it conforms to pattern for the format of this source
	 * @return
	 */
	public boolean isFormatMatch(String path) {
		if (defaultFormatRegex == null) return false;
		Matcher m = defaultFormatRegex.matcher(path);
		return m.find();
	}
		
	protected final static int FILLABLE_BUFFER_SIZE = 1024 * 512;

	@Override
	public RecordSource getSource(OpArgs args) throws IOException, SyntaxError {
	    return null;
	}
}
