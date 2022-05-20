package com.amazon.djk.test;

import java.io.IOException;
import java.util.List;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.processor.JackKnife;
import com.amazon.djk.record.Record;

public class ExpressionNumSunkTest implements ExpressionTest {
    private final JackKnife knife;
    private final String exp;
    private final String compareSymbol;
    private final int expectedNumSunk;

    /**
     * 
     * @param knife
     * @param exp the test expression
     * @param resp the response expression or null if non-existent
     */
    public ExpressionNumSunkTest(JackKnife knife, String exp, String compareSymbol, int expectedNumSunk) {
        this.knife = knife;
        this.exp = exp;
        this.compareSymbol = compareSymbol;
        this.expectedNumSunk = expectedNumSunk;
    }

    @Override
    public boolean isSuccessful() throws IOException, SyntaxError {
        List<Record> recs = knife.collectMain(exp);
        
        switch (compareSymbol) {
        case "=":
        case "==":
            if (recs.size() != expectedNumSunk) {
                System.err.println("incorrect numSunk for expression: " + exp);
                return false;
            }
            return true;
                
        case ">":
            if (recs.size() <= expectedNumSunk) {
                System.err.println("incorrect numSunk for expression: " + exp);
                return false;
            }
            return true;
            
        case ">=":
            if (recs.size() < expectedNumSunk) {
                System.err.println("incorrect numSunk for expression: " + exp);
                return false;
            }
            return true;
                
        case "<":
            if (recs.size() >= expectedNumSunk) {
                System.err.println("incorrect numSunk for expression: " + exp);
                return false;
            }
            return true;
            
        case "<=":
            if (recs.size() > expectedNumSunk) {
                System.err.println("incorrect numSunk for expression: " + exp);
                return false;
            }
            return true;
                
        default:
            System.err.println("unsupported compare symbol: " + compareSymbol);
            return false;
        }
        
    }
    
}

