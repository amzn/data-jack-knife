package com.amazon.djk.report;

import com.amazon.djk.processor.ExecutionContext;

public class ProgressReportFactory {
	public static NodeReport create(ReportProvider provider) {
		return provider instanceof ExecutionContext ? 
		        new ProgressReport((ExecutionContext)provider) :
		        new NodeReport(provider);
	}
	
	public static NodeReport create(ReportProvider provider, String nodeName) {
		return provider instanceof ExecutionContext ?
		        new ProgressReport((ExecutionContext)provider) : 
		        new NodeReport(provider, nodeName);
	}
}
