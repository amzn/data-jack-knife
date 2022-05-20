package com.amazon.djk.source;

import java.io.IOException;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;

/**
 * 
 * for display purposes
 *
 */
public class ElseReceiver extends ReceiverSource implements Keyword {
	public static final String ELSE_TOKEN = "else"; 
	
	@Description(
	        text={"Delimits the 'else' clause within an 'if' expression. See 'if'"},
	        contexts={"[ TRUE_EXP else FALSE_EXP if"})
    public static class Op extends SourceOperator {
    	public Op() {
			super(ELSE_TOKEN, Type.NAME);
		}

    	@Override
    	public RecordSource getSource(OpArgs args) throws IOException, SyntaxError {
    		return new ElseReceiver();
    	}
    }
}
