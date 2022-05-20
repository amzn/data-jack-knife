package com.amazon.djk.expression;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobProperty {
	private static final Logger LOG = LoggerFactory.getLogger(JobProperty.class);
	private static final Pattern PROPERTY_PATTERN = Pattern.compile("^(?<name>[a-zA-Z_]+?)\\s*=\\s*(?<value>[^;]+);\\s*");

	public final String nonNamespacedName;
	public final String unresolvedValue;
	public String resolvedValue = null;
	
	public JobProperty(String nonNamespacedName, String unresolvedValue) {
		this.nonNamespacedName = nonNamespacedName;
		this.unresolvedValue = unresolvedValue;
	}
	
	public static JobProperty create(String chunk) throws SyntaxError {
		Matcher m = PROPERTY_PATTERN.matcher(chunk);
		if (!m.matches()) return null;
		
		String name = m.group("name");
		String value = m.group("value");
		return new JobProperty(name, value);
	}
	
	public void setResolvedValue(String resolvedValue) {
		this.resolvedValue = resolvedValue;
	}
	
	@Override
	public String toString() {
		return String.format("%s = %s", nonNamespacedName, resolvedValue != null ? resolvedValue : unresolvedValue);
	}
}
