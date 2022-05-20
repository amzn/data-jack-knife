package com.amazon.djk.manual;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * annotation for Operators
 *  * 
 * @author mschultz
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Examples.class)
public @interface Example
{
	/**
	 * 
	 * @return the expression example for this operator
	 */
	String expr();
	
	/**
	 * 
	 * @return the ExampleType (DISPLAY_ONLY or EXECUTABLE)
	 */
	ExampleType type() default ExampleType.DISPLAY_ONLY;
}
