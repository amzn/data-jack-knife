package com.amazon.djk.reducer;

import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.PipeOperator;


@Param(name=ReducerOperator.INSTANCE_PARAM, gloss="An instance name for main expressions only.  Used to send this reduction to a reduction expression. E.g. REDUCE:current,new", type=ArgType.STRING)
public abstract class ReducerOperator extends PipeOperator {
	public final static String INSTANCE_PARAM = "instance";
	public ReducerOperator(String usage) {
		super(usage);
	}
}
