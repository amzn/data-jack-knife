package com.amazon.djk.manual;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** references:
 * https://en.wikipedia.org/wiki/Box-drawing_character
 * http://www.termsys.demon.co.uk/vtansi.htm
 */
public class Manual extends Display {
	protected final StringBuilder manual = new StringBuilder();
	
	public void clear() {
		manual.setLength(0);
	}

	private final Pattern allCaps = Pattern.compile("[A-Z]{2,}");
    protected String colorAllCaps(String string, String color) {
    	if (getDisplayType() == DisplayType.DEFAULT) return string;

    	StringBuilder sb = new StringBuilder();
    	Matcher m = allCaps.matcher(string);
    	int end = 0;
    	while (m.find()) {
    		int start = m.start();
    		sb.append(string, end, start); // up to here    		
    		end = m.end();
    		sb.append(color);
    		sb.append(string, start, end);
    		sb.append(endColor());
    	}
    	if (sb.length() == 0) return string;
    	if (string.length() > end) sb.append(string, end, string.length());
    	
    	return sb.toString();
    }
    
    public void addString(String string, String color) {
    	manual.append(color);
    	manual.append(string);
    	manual.append(endColor());
    }
    
    public void addLine(String line, String color) {
    	addString(line + "\n", color);
    }
    
    public void addString(String string) {
    	manual.append(string);
    }
    
    public void addLine(String line) {
    	manual.append(line);
    	manual.append('\n');
    }
    
    public void addLine() {
    	manual.append('\n');
    }
    
    public void display() {
    	System.out.print(manual.toString());
    }
    
    public String getAsString() {
    	return manual.toString();
    }
    
    public List<String> getAsList() {
    	String s = getAsString();
    	String[] lines = s.split("\n");
    	return Arrays.asList(lines);
    }
    
    /**
	 * 
	 * @param level
	 */
	public int indent(int level) {
		int indentCharsPerLevel = 3;
		for (int i = 0; i < level; i++) {
			addString("   "); // 3 spaces
		}
		
		return indentCharsPerLevel * level;
	}
}
