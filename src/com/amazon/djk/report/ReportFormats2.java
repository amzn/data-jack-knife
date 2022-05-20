package com.amazon.djk.report;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * annotation for RecordSources for defining additional ReportLines for SubClasses
 * if the super class contains a ReportLines annotation, these will also be used.
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface ReportFormats2
{
	/**
	 * 
	 * @return
	 */
    String headerFormat() default "";
    
    /**
     * 
     * @return
     */
	String[] lineFormats() default {};
	
	/**
     * this could be made to per line with an array.
     * 
     * @return
     */
    int maxLineLength() default ReportFormats.MAX_LINE_LEN;
}
