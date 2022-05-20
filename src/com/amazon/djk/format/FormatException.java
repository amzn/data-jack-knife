package com.amazon.djk.format;

import java.io.IOException;

/**
 * Thrown by FormatParsers for parsing errors 
 *
 */
public class FormatException extends IOException {
	public FormatException(String message) {
		super(message);
	}

	public FormatException(Exception rootCause) {
		super(rootCause);
	}

	public FormatException(String message, Exception rootCause) {
		super(message, rootCause);
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
}
