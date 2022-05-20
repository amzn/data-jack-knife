package com.amazon.djk.processor;

import java.io.IOException;

public class ForceDoneException extends IOException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1097833712379076841L;

	public ForceDoneException(String message) {
		super(message);
	}
	
	public ForceDoneException() {
		super();
	}
}
