package com.amazon.djk.format;

import java.io.File;
import java.io.IOException;

import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.Param;
import com.amazon.djk.file.FileOperator;
import com.amazon.djk.file.FormatArgs;

@Arg(name="PATH", gloss="the location of the data store.  Use an extension hint to specify the format.", type=ArgType.STRING, eg="tsv")
@Param(name=WriterOperator.USE_GZIP_PARAM, gloss="If false, data written in clear text.", type=ArgType.BOOLEAN, defaultValue = "true")
@Param(name=WriterOperator.AS_FILE_PARAM, gloss="If true, a single file will be created by a single thread.", type=ArgType.BOOLEAN, defaultValue = "false")
@Param(name=WriterOperator.OVERWRITE_PARAM, gloss="If true, previous data will be overwritten.", type=ArgType.BOOLEAN, defaultValue = "false")
@Param(name=WriterOperator.PATH_REDUCER_INSTANCE_PARAM, gloss="If a path-reducer-instance name is provided, a reducer producing a single record with a 'path' field is created", type=ArgType.STRING)
public abstract class WriterOperator extends FileOperator {
	public final static String AS_FILE_PARAM = "asFile";
	public final static String USE_GZIP_PARAM = "gzip";
	public final static String OVERWRITE_PARAM = "overwrite";
	public final static String PATH_REDUCER_INSTANCE_PARAM = "pri";
	
	public WriterOperator(String format, String streamFileRegex) {
		super(format, streamFileRegex);
	}
	
	public abstract FormatWriter getWriter(FormatArgs args, File dataFile) throws IOException;
}
