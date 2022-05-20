package com.amazon.djk.processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.CommaList;
import com.amazon.djk.expression.Expression;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.sink.QueueSink;

/**
 * Context for collecting sunk records of MAIN and REDUCE 
 */
public class CollectionContext extends ExecutionContext {
	private RecordSink mainQueueSink = null;
	private Map<CommaList,RecordSink> reducerQueueSinks = null;
	
	public CollectionContext(InnerKnife knife, RecordSource operand, Expression expr) throws SyntaxError, IOException {
		super(knife, operand, expr);
	}
	
	/**
	 * 
	 * @param knife
	 * @param exp
	 * @return
	 * @throws IOException
	 */
	public static CollectionContext create(InnerKnife knife, Expression exp) throws IOException {
		return new CollectionContext(knife, null, exp);
	}
	
	/**
	 * 
	 * @param knife
	 * @param upstream
	 * @param exp
	 * @return
	 * @throws IOException
	 */
	public static CollectionContext create(InnerKnife knife, RecordSource upstream, Expression exp) throws IOException {
		return new CollectionContext(knife, upstream, exp);
	}
	
	public RecordSink getOriginalMainSink() throws IOException {
		return super.getMainSink();
	}
	
	public RecordSink getMainSink() throws IOException {
		if (mainQueueSink != null) return mainQueueSink;

		mainQueueSink = new QueueSink();
		RecordSink actualSink = super.getMainSink();;
		mainQueueSink.addSource(actualSink.getSource()); // toss the main sink
		return mainQueueSink;
	}
	
	@Override
	public Map<CommaList,RecordSink> getReduceSinks() throws IOException {
		if (reducerQueueSinks != null) return reducerQueueSinks;
		
		reducerQueueSinks = new HashMap<>();
		Map<CommaList,RecordSink> actualSinks = super.getReduceSinks();
		Set<CommaList> keys = actualSinks.keySet();
		for (CommaList key : keys) {
			RecordSink asink = actualSinks.get(key);
			QueueSink replacement = new QueueSink();
			replacement.addSource(asink.getSource()); // toss actual sink
			reducerQueueSinks.put(key, replacement);
		}
	
		return reducerQueueSinks;
	}
}

