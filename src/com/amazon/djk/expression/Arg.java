package com.amazon.djk.expression;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * annotation for Operators
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Args.class)
public @interface Arg
{
	String name();
	
	String gloss();
	
	ArgType type();

	/**
     * 
     * @return an example of the arg for the manual
     */
    String eg() default "";
}
