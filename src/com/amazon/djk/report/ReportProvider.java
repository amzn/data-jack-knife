package com.amazon.djk.report;

public interface ReportProvider {
	/**
	 * 
	 * @return the singleton report for this provider
	 */
	NodeReport getReport();
	
	/**
	 * causes the suppression of this provider's report
	 */
	void suppressReport(boolean value);
	
	/**
	 * 
	 * @return true if this provider's report should be suppressed
	 */
	boolean isReportSuppressed();

	/**
	 * 
	 * @return an up-to-date instance of ProgressData for this provider 
	 */
	ProgressData getProgressData();
}
