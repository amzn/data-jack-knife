package com.amazon.djk.expression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.amazon.djk.pipe.ForeachPipe;
import com.amazon.djk.pipe.IfPipe;
import com.amazon.djk.pipe.InjectPipe;
import com.amazon.djk.source.ReceiverSource;

public class TokenResolver {
    public static final char COMMENT_BEGIN_CHAR = '#'; // needs to be shielded within '' in bash

    /**
     * get expression chunks as a stack of tokens, macros resolved, pipe/sink operators set
     * 
     * @param expressionChunks
     * @param operators
     * @param map 
     * @return
     * @throws SyntaxError
     * @throws IOException
     */
    public static Stack<ParseToken> getAsStack(ExpressionChunks expressionChunks) throws SyntaxError, IOException {
        List<ParseToken> tokenList = getTokens(expressionChunks);
        
        Stack<ParseToken> tokStack = new Stack<>();
        for (int i = tokenList.size() - 1; i >= 0; i--) {
            tokStack.add(tokenList.get(i));
        }
        
        setTokenContexts(tokStack);
        
        return tokStack;
    }
    
    /**
     * parse the expression and identify inKeyContexts.
     * TODO: ideally this should move into the parser itself and Operator
     * should implement isInSubexpression method, not OpArgs.
     * 
     * @param tokens
     */
    private static void setTokenContexts(Stack<ParseToken> tokens) {
        int foreachDepth = 0;
        for (ParseToken token : tokens) {
            String opName = token.getOperator();

            if (isSubExpressionConsumingOperator(opName)) {
                foreachDepth++;
                continue;
            }

            if (opName.equals(IfPipe.NAME) && foreachDepth > 0) {
                foreachDepth++;
                continue;
            }
            
            if (opName.equals(ReceiverSource.LEFT_SCOPE) && foreachDepth > 0) {
                foreachDepth--;
                continue;
            }
            
            if (foreachDepth > 0) {
                token.setInSubExpression(true);
            }
        }
    }
    
    /**
     * currently these are foreach and inject.
     * @param opName
     * @return
     */
    private static boolean isSubExpressionConsumingOperator(String opName) {
        return opName.equals(ForeachPipe.NAME) || opName.equals(InjectPipe.NAME);
    }
    
    /**
     * turn chunks into tokens with parsed slots
     * 
     * @param expressionChunks
     * @param pipeOps
     * @param sourceOps 
     * @return
     * @throws SyntaxError 
     */
    public static List<ParseToken> getTokens(ExpressionChunks expressionChunks) {
        trimComments(expressionChunks);
        List<ParseToken> list = new ArrayList<>();
        int size = expressionChunks.size();
        for (int i = 0; i < size; i++) {
            String chunk = expressionChunks.get(i);
            ParseToken token = new ParseToken(chunk, i, i == size - 1);
            list.add(token);
        }
        
        return list;
    }
    
    /**
     * trim off chunks that are comments
     * 
     * @param expressionChunks
     */
    private static void trimComments(ExpressionChunks expressionChunks) {
        Iterator<String> iter = expressionChunks.iterator();
        while (iter.hasNext()) {
            String chunk = iter.next();
            
            if (chunk.charAt(0) == COMMENT_BEGIN_CHAR) {
                iter.remove();
            }
        }
    }
}
