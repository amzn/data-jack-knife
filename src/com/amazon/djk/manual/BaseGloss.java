package com.amazon.djk.manual;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * annotation for Operators
 * 
 * identical to Gloss/Glosses except inheritable so that a base class
 * can define annotations that get picked up by subclasses, only works
 * one set of inheritance.  The work here is done in the getGlosses()
 * method in OperatorManual
 * 
 *  * 
 * @author mschultz
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(BaseGlosses.class)
@Inherited
public @interface BaseGloss
{
	String entry();
	
	String def() default ""; // optional so not needed when type=Include.GLOBAL
	
	GlossType type() default GlossType.SYNTACTIC;
}
