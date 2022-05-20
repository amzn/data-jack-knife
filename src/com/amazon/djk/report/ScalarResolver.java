package com.amazon.djk.report;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;

/**
 * base implementation for resolver
 * 
 */
public class ScalarResolver {
	protected final Field field;
	protected final String annotatedName;
	protected final double multiplier;
	protected final AggType aggregation;
	protected volatile Object rawValue;
	/*
	 * Types of aggregation across threads
	 */
	public enum AggType{
	    NONE,
        ADD,
        MAX,
        MIN
	}
	
	public ScalarResolver(Field field, String annotatedName, AggType aggregation, double multiplier) {
		this.field = field;
		this.annotatedName = annotatedName;
		this.multiplier = multiplier;
		this.aggregation = aggregation;
	}

	/**
	 * returns a string representation of the resolvers current value
	 */
	@Override
	public String toString() {
		Object v = getValue();
		return v != null ? v.toString() : "<null>";
	}

	/**
	 * 
	 * @return the annotated name of this progress variable
	 */
	public String getAnnotatedName() {
		return annotatedName;
	}
	
	/**
	 * 
	 * @return a stable name for this progress variable.
	 */
	public String getStableName() {
		return field.getName();
	}
	
	/**
	 * sets the raw value
	 * @param target from which the raw value is obtained
	 */
	public void set(ReportProvider target) {
		try {
			field.setAccessible(true);
			rawValue = field.get(target);
		}
		
		catch (IllegalArgumentException | IllegalAccessException e) {
			rawValue = null;
		}
	}
	
	/**
	 * aggregates the raw value from another resolver into this one
	 * 
	 * @param other from which the raw value comes
	 */
	public void aggregate(ScalarResolver other) {
		if (other == null) return;
		
		Object otherRawValue = other.rawValue;
		
		switch(aggregation){
		case NONE:
		    return;
		case ADD:
		    accumulateValues(otherRawValue);
		    return;
		case MAX:
		    aggregateMaxValue(otherRawValue);
		    return;
		case MIN:
		    aggregateMinValue(otherRawValue);
		    return;
		}
	}
	
    /**
	 * 
	 * @return
	 */
	public Object getValue() {
		if (rawValue == null) return null;
		
		// allows annotation of String[] common pattern in args
		if (rawValue instanceof String[]) {
			return StringUtils.join((String[])rawValue, ",");
		}
		
        if (rawValue instanceof AtomicLong) {
            return ((AtomicLong)rawValue).get();
        }
        
        if (rawValue instanceof AtomicInteger) {
            return ((AtomicInteger)rawValue).get();
        }
        
        if (rawValue instanceof AtomicBoolean) {
            return ((AtomicBoolean)rawValue).get();
        }
		
		else if (multiplier == 1.0) return rawValue;
		return scale (rawValue); 
	}
	
	/**
	 * scales by the multiplier, always returns as double
	 * 
	 * @param v parameter to scale
	 * @return
	 */
	private double scale(Object v) {
		return getAsDouble(v) * multiplier;
	}
	
	/**
	 * helper static method
	 * 
	 * @return o as a double
	 */
	public static double getAsDouble(Object o) {
		double d = Double.NaN;
		
		if (o instanceof Long) {
			d = (Long)o;
		}
			
		else if (o instanceof Double) {
			d = (Double)o;
		}
			
		else if (o instanceof Integer) {
			d = (Integer)o;
		}
		
        else if (o instanceof Float) {
            d = (Float)o;
        }

		return d;
	}
	
	private void aggregateMinValue(Object otherRawValue) {
        if (rawValue instanceof Long) {
            rawValue = Math.min((Long) rawValue, (Long) otherRawValue);
        }
        
        else if (rawValue instanceof Double) {
            rawValue = Math.min((Double) rawValue, (Double) otherRawValue);
        }
        
        else if (rawValue instanceof Integer) {
            rawValue = Math.min((Integer) rawValue, (Integer) otherRawValue);
        }
        
        else if (rawValue instanceof Float) {
            rawValue = Math.min((Float) rawValue, (Float) otherRawValue);
        }         
    }

    private void aggregateMaxValue(Object otherRawValue) {
        if (rawValue instanceof Long) {
            rawValue = Math.max((Long) rawValue, (Long) otherRawValue);
        }
        
        else if (rawValue instanceof Double) {
            rawValue = Math.max((Double) rawValue, (Double) otherRawValue);
        }
        
        else if (rawValue instanceof Integer) {
            rawValue = Math.max((Integer) rawValue, (Integer) otherRawValue);
        }
        
        else if (rawValue instanceof Float) {
            rawValue = Math.max((Float) rawValue, (Float) otherRawValue);
        }        
    }

    private void accumulateValues(Object otherRawValue) {
        if (rawValue instanceof Long) {
            rawValue = (Long) rawValue + (Long) otherRawValue;
        }
        
        else if (rawValue instanceof Double) {
            rawValue = (Double) rawValue + (Double) otherRawValue;
        }
        
        else if (rawValue instanceof Integer) {
            rawValue = (Integer) rawValue + (Integer) otherRawValue;
        }
        
        else if (rawValue instanceof Float) {
            rawValue = (Float) rawValue + (Float) otherRawValue;
        }        
    }
}	
