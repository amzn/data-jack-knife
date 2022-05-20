package com.amazon.djk.report;

import java.lang.reflect.Field;


/**
 * resolver for percent of a scalar field relative to another scalar field
 * 
 */
public class PercentResolver extends DependingResolver {
	private ScalarResolver denomResolver;
	
	public PercentResolver(Field field, String annotatedName, AggType aggregation, double multiplier, ScalarResolver denomResolver) {
		super(field, annotatedName, aggregation, multiplier);
		this.denomResolver = denomResolver;  
	}

	@Override
	public String getStableName() {
		return field.getName() + "_percent";
	}
	
	@Override
	public Object getValue() {
		double numer = getAsDouble(super.getValue());
		double denom = getAsDouble(denomResolver.getValue());
		if (denom == 0.0) return Double.NaN;
		return numer * 100.0 / denom;
	}
}	
