package com.amazon.djk.expression;

public class CommaList {
	private final String list;
	private final String[] array;
	
	public CommaList(String commaSeparatedValues) {
		this.list = commaSeparatedValues;
		this.array = list.split(",");
	}
	
	public String[] array() {
		return array;
	}
	
	@Override
	public String toString() {
		return list;
	}
	
	public int length() {
		return array.length;
	}
	
	public int hashCode() {
		return list.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o instanceof CommaList) {
			return ((CommaList)o).list.equals(list);
		}
		
		return false;
	}
}
