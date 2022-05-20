package com.amazon.djk.expression;

import java.io.IOException;

import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.keyed.KeyedSource;

public abstract class SourceOperator extends Operator {
	public enum Type {NAME, USAGE};
	
	/**
	 * e.g. format = "nat" usage = "PATH"
	 * 
	 * @param format
	 * @param usage
	 */
	public SourceOperator(String format, String usage) {
		super(format, usage);
	}
	
	/**
	 * 
	 * @param nameOrUsage either the name or the usage spec depending on type
	 * @param type either type NAME or USAGE
	 */
	public SourceOperator(String nameOrUsage, Type type) {
	    super(getName(nameOrUsage, type), nameOrUsage);
	}
	
	private static String getName(String nameOrUsage, Type type) {
	    return type == Type.NAME ? nameOrUsage : new OpUsage(nameOrUsage).getOpName();
	}
	
	public abstract RecordSource getSource(OpArgs args) throws IOException, SyntaxError;
	
	/**
	 * implemented by SourceOperators extending KeyedSource 
	 * 
	 * @param opArgs
	 * @return
	 * @throws IOException
	 * @throws SyntaxError
	 */
	public KeyedSource getKeyedSource(OpArgs opArgs) throws IOException, SyntaxError {
	    return null;
	}
}
