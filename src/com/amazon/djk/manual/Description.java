package com.amazon.djk.manual;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * annotation for Operators
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Description
{
	/*
	 * explicitly defined context for the operator
	 */
	String[] contexts() default {""};
	
	/*
	 * the text of the description.  Manual may format this anyway it sees fit
	 * do not expect your line breaks to respected.
	 */
	String[] text();
	
	/*
	 * a la the html <pre> tag these lines are displayed in a fixed way
	 * do not use line breaks in within the lines, use one string entry
	 * per line desired.  Lines should be less than 100 characters.
	 * DJK will truncate if they are too long.
	 */
	String[] preLines() default {""}; 
	
	/**
	 * Optional but required by ManPageable classes to identify the topic of the man page. 
	 * @return
	 */
	String topic() default "";
}
