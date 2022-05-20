package com.amazon.djk.report;

import java.lang.reflect.Field;

public abstract class DependingResolver extends ScalarResolver {

	public DependingResolver(Field field, String annotatedName, AggType aggregation, double multiplier) {
		super(field, annotatedName, aggregation, multiplier);
	}
}
