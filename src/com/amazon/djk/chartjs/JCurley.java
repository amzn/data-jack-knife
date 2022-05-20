package com.amazon.djk.chartjs;

import java.util.ArrayList;
import java.util.List;

public class JCurley implements J {
	private final String name;
	private final char begin;
	private final char end;
	private List<J> childs = new ArrayList<>();
	private StringBuilder sb = new StringBuilder();
	
	JCurley(String name) {
		this(name, '{', '}');
	}
	
	JCurley() {
		this(null, '{', '}');
	}
	
	protected JCurley(String name, char begin, char end) {
		this.name = name;
		this.begin = begin;
		this.end = end;
	}

	public J add(J son) {
		childs.add(son);
		return this;
	}
	
	@Override
	public String toString() {
		String open = name != null ? String.format("%s: %c\n", name, begin) : 
			String.format("%c\n", begin); 
		sb.append(open);
		for (J child : childs) {
			sb.append(child);
		}
		sb.append(String.format("%c,\n", end));
		
		return sb.toString();
	}
}
