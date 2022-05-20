package com.amazon.djk.expression;

import java.io.IOException;
import java.util.Stack;

import com.amazon.djk.core.RecordSource;
import com.amazon.djk.keyed.KeyedSource;
import com.amazon.djk.keyed.LazyKeyedSource;

public class ParserOperands {
	private final Stack<RecordSource> operands = new Stack<>();
	
	public void add(RecordSource source) {
		operands.add(source);
	}
	
    public int size() {
        return operands.size();
    }

    public RecordSource peek() {
        return operands.size() == 0 ? null : operands.peek();
    }

    /**
     * pops the current RecordSource from the internal stack.  If it is a LazySource instantiation is performed
     * 
     * @return
     * @throws SyntaxError
     * @throws IOException 
     */
	public RecordSource pop() throws SyntaxError, IOException {
        if (operands.size() < 1) throw new SyntaxError("too few operands on stack");
        RecordSource source = operands.pop();
		
        if (source instanceof LazyKeyedSource) {
            return ((LazyKeyedSource)source).getSource();
        }
        
        return source;
	}
	
    /**
     * pops the current RecordSource from the internal stack.  If it is a LazySource instantiation is performed
     * 
     * @return
     * @throws SyntaxError
     * @throws IOException 
     */
    public KeyedSource popAsKeyedSource() throws IOException, SyntaxError {
        if (operands.size() < 1) throw new SyntaxError("too few operands on stack");
        RecordSource source = operands.pop();

        if (source instanceof KeyedSource) {
            return (KeyedSource)source;
        }

        if (source instanceof LazyKeyedSource) {
            return ((LazyKeyedSource)source).getKeyedSource();
        }
        
        throw new SyntaxError("operand stack source is not keyed");
    }
    
    /**
     * pops the current RecordSource as is from the internal stack without Lazy instantiation
     * 
     * @return
     * @throws SyntaxError
     * @throws IOException 
     */
    public RecordSource popSourceAsIs() throws IOException, SyntaxError {
        if (operands.size() < 1) throw new SyntaxError("too few operands on stack");
        return operands.pop();
    }
}
