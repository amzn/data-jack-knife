package com.amazon.djk.report;


import java.lang.reflect.Field;
import java.util.Date;

/**
 * resolver for rate of change of a scalar field
 * 
 * @author mschultz
 *
 */
public class RateResolver extends ScalarResolver {
	private final long startMillis;
	private double lastValue = Double.NEGATIVE_INFINITY;
	private double lastRate = Double.NEGATIVE_INFINITY;

	/**
	 * 
	 * @param field
	 * @param annotatedName
	 * @param aggregation
	 */
	public RateResolver(Field field, String annotatedName, AggType aggregation, double multiplier) {
		super(field, annotatedName, aggregation, multiplier);
		startMillis = new Date().getTime();
	}

	@Override
	public String getStableName() {
		if (multiplier != 1.0) {
			return field.getName() + String.format("_x_%f_perSec", multiplier);
		}
		return field.getName() + "_perSec";
	}
	
	@Override
	public Object getValue() {
		long millis = (new Date().getTime() - startMillis);
		if (millis == 0) return Double.NaN; 
		double value = getAsDouble(super.getValue());
		double rate = value / millis * 1000.0;
		
		// if value finishes changing (or even stalls) don't update the rate
		// so we get a more accurate picture of rates.  Any further delta in
		// value will cause the rate to catch up appropriately
		if (value == lastValue) {
			return lastRate;
		}
		
		lastValue = value;
		lastRate = rate;
		
		return rate;
	}
}	
