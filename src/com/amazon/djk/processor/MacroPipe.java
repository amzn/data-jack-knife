package com.amazon.djk.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.ChunkTokenizer;
import com.amazon.djk.expression.CommaList;
import com.amazon.djk.expression.Expression;
import com.amazon.djk.expression.ExpressionChunks;
import com.amazon.djk.expression.SyntaxError;

/**
 * This class is never extended.  To implement a macro all that is needed is to extend
 * MacroOperator.
 */
public final class MacroPipe extends RecordPipe {
	public static final String MACRO_SOURCE_VARIABLE = "?";
	private final ExpressionChunks macroMainChunks;
	private final List<RecordSource> sources = new ArrayList<>();
	private final int arity;
	
	public MacroPipe(List<String> rawMacroLines) throws SyntaxError, IOException {
	    super(null);
	    
	    String macroString = getMacroString(rawMacroLines);
	    String[] allChunks = ChunkTokenizer.split(macroString);
	    Expression expr = Expression.create(allChunks);

	    ExecutionContext.declareFields(expr);
	    ExecutionContext.setKnifeProperties(expr);
	    
	    macroMainChunks = expr.getMainChunks();
	    Map<CommaList, ExpressionChunks> reducers = expr.getReduceExpressionChunks();
	    ExpressionChunks report = expr.getReportChunks();

	    // it is not allowed to define REDUCE or REPORT macros within a macro, only MAIN
	    if (reducers.size() != 0 || report.size() != 0) {
	    	throw new SyntaxError("neither REDUCE nor REPORT macros are allowed within file based macro");
	    }
	    
		int num = 0;
		Iterator<String> iter = macroMainChunks.iterator();
        while (iter.hasNext()) {
        	String token = iter.next();
            if (token.equals(MACRO_SOURCE_VARIABLE)) {
                num++;
            }
        }
		
        this.arity = num;
	}
	
	/**
     * helper method to convert lines of valid macro to a single macro string
     * strips comments, etc.
     * 
     * @param rawMacroLines
     * @return
     */
    public static String getMacroString(List<String> rawMacroLines) {
        StringBuilder sb = new StringBuilder();

        for (String line : rawMacroLines) {
            // everything after # is a comment
            int hash = line.indexOf('#');
            if (hash != -1) {
                line = line.substring(0, hash);
            }

            sb.append(line);
            sb.append(' ');
        }

        return sb.toString();
    }
	
	/**
	 * 
	 * @return the number of operands this macro consumes.
	 */
	public int getArity() {
	    return arity;
	}
	
	public ExpressionChunks getMainChunks() {
	    return macroMainChunks;
	}

	@Override
	public RecordPipe addSource(RecordSource source) {
	    sources.add(source);
	    return this;
	}
	
	public RecordSource popSource() {
		return sources.remove(sources.size()-1);
	}
}
