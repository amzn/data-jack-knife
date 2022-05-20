package com.amazon.djk.manual;

import com.amazon.djk.expression.Operator;
import com.amazon.djk.expression.ChunkTokenizer;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Display.DisplayType;
import com.amazon.djk.source.ReceiverSource;

public class SyntaxSection {
	private static final int TREE_PRINT_MAX_LINE_LEN_DEFAULT = 50;
	private int treeMaxLineLen = TREE_PRINT_MAX_LINE_LEN_DEFAULT;
	private boolean lastWasNewLine = true;
	private int lineCharLen = 0;
	private final Manual manual;
	
	public SyntaxSection(Manual manual) {
		this.manual = manual;
	}
	
	public void setTreeMaxLineLen(int maxLen) {
		treeMaxLineLen = maxLen;
	}
	
	private void printTreeNewLine(int level) {
		manual.addLine();
		lastWasNewLine = true;
		lineCharLen = 0;
		manual.indent(level);
	}

	/**
     * 
     * @param exp
     */
    public void displayExpAsTree(String expr) {
        String[] tokens = ChunkTokenizer.split(expr);
        displayExpAsTree(tokens, -1);
    }

	/**
	 * 
	 * @param exp
	 * @param highlightTokenNo if not -1, that token will be highlighted
	 */
	public void displayExpAsTree(String[] exp, int highlightTokenNo) {
		int level = 0;
		lastWasNewLine = true;
		lineCharLen = 0;
		
		for (int i = 0; i < exp.length; i++) {
			String token = exp[i];
			boolean highlight = (i == highlightTokenNo);
			
			// for System.properties
			if (token.endsWith(";")) {
				manual.addLine(token);
                lastWasNewLine = true;
			}
			
			else if (isLeftScopedPredicate(token)) {
				printTreeNewLine(--level);
				printToken(token, highlight);
				
				// look ahead for adjacent leftScoper
				if (i == exp.length - 1 || !isLeftScopedPredicate(exp[i+1])) {
                    printTreeNewLine(level);
				}
			}
			
            else if (token.startsWith("else")) {
                printTreeNewLine(--level);
                printToken(token, highlight);
                printTreeNewLine(++level);
            }
			
			else if (token.equals(ReceiverSource.LEFT_SCOPE)) {
				if (!lastWasNewLine) {
					printTreeNewLine(level);
				}

                printToken(token, highlight);
				printTreeNewLine(++level);
			}
			
			else {
				lastWasNewLine = false;
				if (lineCharLen != 0 && lineCharLen + token.length() > treeMaxLineLen ) {
					printTreeNewLine(level);
				}
				
				lineCharLen += token.length();
				printToken(token, highlight);
				manual.addString(" ");
                lastWasNewLine = false;
			}
		}
	}
	
	/**
	 * 
	 * @param op
	 * @return true if predicate needs a left-scope [
	 */
	private boolean isLeftScopedPredicate(String op) {
		return (op.startsWith("if:") ||
                op.startsWith("ifNot:") ||
                op.startsWith("foreach:") ||
                op.startsWith("inject") ||
	            op.equals("]"));
	}
	
	/**
	 * 
	 * @param token
	 * @param highlight
	 */
	private void printToken(String token, boolean highlight) {
		if (!highlight) {
			manual.addString(token);
			return;
		}
		
		if (manual.getDisplayType() != DisplayType.DEFAULT) {
			manual.addString(token, manual.red());
		}
		
		else {
			manual.addString(">>>" + token + "<<<");
		}
	}
	
	/**
	 * 
	 * @param op
	 * @param description
	 */
	public void display(Operator op, String[] contexts) {
		manual.addLine("syntax:", manual.lightblue());
		manual.addLine();
		
		boolean hasContext = false;
		for (String c : contexts) {
		    if (c.length() != 0) {
		        if (hasContext) manual.addLine("or");
		        manual.addLine(c);
		        hasContext = true;
		    }
		}
		    
		if (hasContext) {
		    manual.addLine();
		    return; // used explicitly provided context from the description
		}
		    
		if (op.hasInnerSink()) {
		    manual.addLine("... " + op.getUsage() + " ...");               
		}
		    
		else { // just source
		    manual.addLine(op.getUsage() + " ...");
		}
		
        manual.addLine();
	}
	
	/**
	 * 
	 * @param error
	 * @param cmd
	 */
	public void addError(SyntaxError error, String[] cmd) {
        String op = error.getOp();
        if (op != null) {
            manual.addLine(String.format("'%s' syntax incorrect", op), manual.red());
            manual.addLine();
        }
        
        manual.addLine(error.getMessage());
        manual.addLine();

        if (op == null) return; 
			
		int tokenNo = error.getTokenNo();
		displayExpAsTree(cmd, tokenNo);
		manual.addLine();
	}
}
