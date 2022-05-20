package com.amazon.djk.report;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * annotation for RecordSources for defining ReportLines
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface ReportFormats
{
	public static final int MAX_LINE_LEN = 100;
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
     * @return the maximum line length (greater than which line will be wrapped)
     */
    int maxLineLength() default MAX_LINE_LEN;
}
