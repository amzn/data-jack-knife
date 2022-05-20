package com.amazon.djk.expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ChunkTokenizer {

    /**
     * take and expression string and parse it into args(chunks) as the shell would
     * 
     * @param string
     * @return 
     */
    public static String[] split(String string) {
    	return split(string, false);
    }
    
    private static final char ESCAPE_CHAR = '\\';
    private static final char[] beginQuoteChars = {'\'', '"', '{'};
    private static final char[] endQuoteChars = {'\'', '"', '}'};

    private static boolean isBeginQuoteChar(char c) {
    	for (int i = 0; i < beginQuoteChars.length; i++) {
    		if (c == beginQuoteChars[i]) return true;
    	}
    	
    	return false;
    }
    
    private static char getEndQuoteChar(char beginQuoteChar) {
    	for (int i = 0; i < beginQuoteChars.length; i++) {
    		if (beginQuoteChar == beginQuoteChars[i]) {
    			return endQuoteChars[i];
    		}
    	}
    	
    	return ' ';
    }
    
    /**
     * 
     * @param string
     * @param keepTicks if true will retain the ticks
     * @return
     */
    public static String[] split(String string, boolean keepTicks) {
        List<String> chunks = new ArrayList<String>();
        boolean isWithinEscaping = false;
        boolean isWithinQuoting = false;
        char endQuoteChar = ' ';
        
        // map on whether the quote chars should be kept in the output chunks
        Map<Character,Boolean> keepQuoteChar = new HashMap<>();
        keepQuoteChar.put('}', true); 
        keepQuoteChar.put('{', true);
        keepQuoteChar.put('"', false);
        keepQuoteChar.put('\'', keepTicks);
        
        
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);

            // escaping achieved with \ char
            if (isWithinEscaping) {
                current.append(c);
                isWithinEscaping = false;
            } 
            
            else if (c == ESCAPE_CHAR && !(isWithinQuoting && endQuoteChar == '\'')) {
            	if (keepTicks) {
            		
            	}
                isWithinEscaping = true;
            }
            
            else if (isWithinQuoting && c == endQuoteChar) {
                if (keepQuoteChar.get(c)) {
                	current.append(c);
                }
                isWithinQuoting = false;
            } 
            
            else if (!isWithinQuoting && isBeginQuoteChar(c)) {
                isWithinQuoting = true;
                if (keepQuoteChar.get(c)) {
                	current.append(c);
                }
                
                endQuoteChar = getEndQuoteChar(c);
            } 
            
            // outside of quoting we break on whitespace
            else if (!isWithinQuoting && Character.isWhitespace(c)) {
                if (current.length() > 0) {
                    chunks.add(current.toString());
                    current = new StringBuilder();
                }
            } 
            
            else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            chunks.add(current.toString());
        }
        
        return chunks.toArray(new String[0]);
    }
    
    /**
     * 
     * @param tickString a string that looks identical to what can be placed within single ticks at the command line.
     * can this even be done? "
     * @return the actual string.
     */
    public static String getShellTickString(String tickString) {
    
        return null;
    }
}
