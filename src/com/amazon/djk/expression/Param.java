package com.amazon.djk.expression;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * annotation for Parameters
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Params.class)
public @interface Param
{
	String name();
	
	String gloss();
	
	ArgType type();

	String UNDEFINED_DEFAULT = "__undefined__";
	String REQUIRED_FROM_COMMANDLINE = "__required__";
	String defaultValue() default  UNDEFINED_DEFAULT;
	
	/**
	 * 
	 * @return an example of the param for the manual
	 */
	String eg() default "";
}
