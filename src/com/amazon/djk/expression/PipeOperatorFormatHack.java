package com.amazon.djk.expression;

import java.io.IOException;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.source.FormatFactory;

/**
 * This class is a hack that should not be used beyond the S3FormatSink 
 * (which should be implemented using the S3FileSystem)
 *
 */
public abstract class PipeOperatorFormatHack extends PipeOperator {

	public PipeOperatorFormatHack(String usage) {
		super(usage);
	}

	public abstract RecordPipe getAsPipe(ParserOperands operands, OpArgs args, FormatFactory factory) throws IOException, SyntaxError;

}
