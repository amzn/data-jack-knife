package com.amazon.djk.record;

import com.amazon.djk.expression.SyntaxError;

public class IllegalFieldException extends SyntaxError {
    public IllegalFieldException(String message) {
        super(message);
    }
}
