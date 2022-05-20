package com.amazon.djk.chartjs;

public class JField implements J {
	private final String name;
	private final String value;
	
	public JField(String name, String value) {
		this.name = name;
		this.value = value;
	}
	
	public J add(J son) {
		throw new UnsupportedOperationException("hey, you can't add things to fields!");
	}

	@Override
	public String toString() {
		return String.format("%s: %s,\n", name, value);
	}
}
