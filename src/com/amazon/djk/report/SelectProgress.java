package com.amazon.djk.report;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.amazon.djk.report.ScalarResolver.AggType;

/**
 * annotation for percent of scalar field relative to another.  This annotation
 * results in a value for the format which is a double regardless of the input
 * types.
 * 
 * @author mschultz
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SelectProgress
{
	// the name of the annotation
	String name();
	
	// whether field with this annotation should be aggregated across strands
	AggType aggregate() default AggType.ADD;

	// for scaling
	public double multiplier() default 1.0;

	// name of the annotated other field
	String otherAnnotation();

	public enum Choice {MIN, MAX};
	// choose the min/max of the fields
	Choice choice();
}
