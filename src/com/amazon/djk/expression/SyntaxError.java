package com.amazon.djk.expression;

import java.io.IOException;

public class SyntaxError extends IOException {
	private String message = "Syntax error";
	private String opString = null;
	private int tokenNo = -1;
	
	public SyntaxError(String message) {
		super(message);
		this.message = message;
	}
	
	public SyntaxError(ParseToken token, String message) {
	    this.opString = token.getOperator();
	    this.tokenNo = token.getTokenNo();
	    this.message = message;
    }

    /**
	 * 
	 * @return
	 */
	public int getTokenNo() {
		return tokenNo;
	}

	/**
	 * 
	 * @return
	 */
	public String getOp() {
		return opString;
	}
	
	@Override
	public String getMessage() {
		return message;
	}
}
