package com.amazon.djk.chartjs;

public interface J {
	public enum Type {CURLEY, SQUARE, FIELD};
	
	public String toString();
	
	public J add(J son);
	
	public static J curley(String name) {
		return new JCurley(name);
	}
	
	public static J curley() {
		return new JCurley();
	}
	
	public static J square(String name) {
		return new JSquare(name);
	}
	
	public static J field(String name, String value) {
		return new JField(name, value);
	}
	
	public static J field(String name, int value) {
		return new JField(name, Integer.toString(value));
	}
	
	public static J field(String name, long value) {
		return new JField(name, Long.toString(value));
	}
	
	public static J field(String name, float value) {
		return new JField(name, Float.toString(value));
	}
	
	public static J field(String name, double value) {
		return new JField(name, Double.toString(value));
	}	
}
