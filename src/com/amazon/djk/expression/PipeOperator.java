package com.amazon.djk.expression;

import java.io.IOException;

import com.amazon.djk.core.RecordPipe;

public abstract class PipeOperator extends Operator {
	public PipeOperator(String usage) {
		super(usage);
	}
	
	public abstract RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError;
}
