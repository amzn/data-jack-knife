package com.amazon.djk.report;

import java.util.Calendar;

/**
 * resolver for the special <date()> annotation 
 * 
 * e.g. the annotation '<date()>%1$te %1$tb %1$tY %1$tT' resolves to '11 Feb 2015 09:31:29'
 * 
 * @author mschultz
 *
 */
public class DateResolver extends ScalarResolver {
	private final Calendar calendar;
	
	public DateResolver() {
		super(null, "date()", AggType.NONE, 1.0);
		calendar = Calendar.getInstance();
	}
	
	@Override
	public String toString() {
		return rawValue != null ? String.format("%1$te.%1$tB.%1$tY %1$tT", rawValue) : "<null>";
	}

	@Override
	public String getStableName() {
		return "date";
	}

	@Override
	public void set(ReportProvider target) {
		rawValue = calendar;
	}
}	
