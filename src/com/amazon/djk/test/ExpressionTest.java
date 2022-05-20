package com.amazon.djk.test;

import java.io.IOException;

import com.amazon.djk.expression.SyntaxError;

public interface ExpressionTest {
    boolean isSuccessful() throws IOException, SyntaxError;
}