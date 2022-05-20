package com.amazon.djk.processor;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.amazon.djk.core.RecordSink;
import com.amazon.djk.expression.CommaList;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ProgressReport;
import com.amazon.djk.sink.QueueSink;

public class CollectionResults {
	private final ProgressReport report;
	private final CollectionContext context;
	
	private List<Record> mainResults = null;
	private Map<CommaList,List<Record>> reduceResults = null;
	
	public CollectionResults(ProgressReport report, CollectionContext context) {
		this.report = report;
		this.context = context;
	}
	
	public ProgressReport getReport() {
		return report;
	}
	
	public List<Record> getMainResults() throws IOException {
		if (mainResults != null) return mainResults;
		
		RecordSink sink = context.getMainSink();
		
		if (! (sink instanceof QueueSink)) {
			throw new SyntaxError("improper configuration");
		}

		QueueSink queueSink = (QueueSink)sink;
		mainResults = queueSink.getRecords();

		return mainResults;
	}
	
	public Map<CommaList, List<Record>> getReduceResults() throws IOException {
		if (reduceResults != null) return reduceResults;
		
		Map<CommaList,RecordSink> sinks = context.getReduceSinks();
		
		reduceResults = new HashMap<>();
		Set<CommaList> keys = sinks.keySet();
		for (CommaList key : keys) {
			RecordSink sink = sinks.get(key);
			
			if (! (sink instanceof QueueSink)) {
				throw new SyntaxError("improper configuration");
			}	

			QueueSink queueSink = (QueueSink)sink;
			reduceResults.put(key, queueSink.getRecords());
		}
		
		return reduceResults;
	}
	
	public List<Record> getReduceResults(CommaList instance) throws IOException {
		Map<CommaList, List<Record>> results = getReduceResults();
		return results.get(instance);
	}
}
