package com.amazon.djk.report;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.amazon.djk.report.ScalarResolver.AggType;

/**
 * annotation for percent of scalar field relative to another
 * 
 * @author mschultz
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface PercentProgress
{
	// the name of the annotation
	String name();
	
	// whether to be aggregated across strands
	AggType aggregate() default AggType.ADD;

	// for scaling the numerator, i.e. the member annotated with @PercentProgress
	double multiplier() default 1.0;
	
	// name of the annotated denominator
	String denominatorAnnotation();
}
