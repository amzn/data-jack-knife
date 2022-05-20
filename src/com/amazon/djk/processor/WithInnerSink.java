package com.amazon.djk.processor;

import java.io.IOException;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.core.RecordSink;

/**
 * Interface that for Sources that contain an inner sink.  The framework
 * will sink the source added with the addSource() method which always
 * called with the current top of the ParserOperands stack.  It is the
 * responsibility of the implementer of a Source implementing WithInnerSink
 * to inject a DevNullSource if no source is needed. (this requirement stems
 * from LazyKeyedSource which delays instantiation of KeyedSource and always
 * grabs one source from the operands stack).
 *
 */
public interface WithInnerSink {
	/**
	 * 
	 * @return the innerSink
	 */
	RecordSink getSink();

	/**
	 * finishing step preparing so the source will be ready to next()
	 * 
	 * @param processor in case the processor is needed to prepare
	 * @throws SyntaxError 
	 * @throws IOException 
	 */
	void finishSinking(InnerKnife processor) throws IOException, SyntaxError;
}
