package com.amazon.djk.format;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Stack;

public class PushbackLineReader {
    public static final int size = 1024 * 1024 * 1;
	private final BufferedReader reader;
	private final Stack<String> lineStack;
    
	public PushbackLineReader(Reader in) {
		reader = new BufferedReader(in, size);
		lineStack = new Stack<>();
	}

	public PushbackLineReader(Stack<String> lineStack) {
	    this.lineStack = lineStack;
	    reader = null;
	}
	
	public void pushBack(String line) {
	    lineStack.push(line);
	}
	
	public String readLine() throws IOException {
		if (!lineStack.isEmpty()) {
		    return lineStack.pop();
		}
		
		if (reader != null) {
		    return reader.readLine();
		}
		
		return null;
	}
	
	public void close() throws IOException {
	    if (reader != null) {
	        reader.close();
	    }
	}
}
