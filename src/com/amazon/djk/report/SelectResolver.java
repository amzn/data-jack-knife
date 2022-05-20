package com.amazon.djk.report;

import java.lang.reflect.Field;

import com.amazon.djk.report.SelectProgress.Choice;

/**
 * resolver for picking min/max of two annotated fields
 * 
 * @author mschultz
 *
 */
public class SelectResolver extends DependingResolver {
	private final ScalarResolver otherResolver;
	private final Choice choice; 
	
	public SelectResolver(Field field, String annotatedName, AggType aggregation, double multiplier, ScalarResolver otherResolver, Choice choice) {
		super(field, annotatedName, aggregation, multiplier);
		this.otherResolver = otherResolver;
		this.choice = choice;
	}

	@Override
	public String getStableName() {
		return field.getName() + "_" + otherResolver.getStableName() + "_"+ choice;
	}
	
	@Override
	public Object getValue() {
		double a = getAsDouble(super.getValue());
		double b = getAsDouble(otherResolver.getValue());
		return choice == Choice.MAX ? Math.max(a, b) :
			Math.min(a, b);		
	}
}	
