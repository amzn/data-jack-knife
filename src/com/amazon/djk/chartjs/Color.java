package com.amazon.djk.chartjs;

public class Color {
	private final String color; 
	
	// backgroundColor: "rgba(255,0,0,1.0)"
	public static Color rgba(int r, int g, int b, float a) {
		return new Color(String.format("rgba(%d,%d,%d,%2.1f)", r, g, b, a));
	}
	
	private Color(String color) {
		this.color = color;
	}
	
	@Override
	public String toString() {
		return color;
	}
}
