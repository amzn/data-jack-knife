package com.amazon.djk.test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.processor.JackKnife;

/**
 * this asserter looks for Expression/Response pairs of the form:
 * E <djk-expression>
 * R <djk-expression>
 * 
 * or
 * E <djk-expression>
 * numSunk SYMB NUM
 * 
 * where SYMB -> {<,>,=,<=,>=}
 * 
 * 
 * If the response line is missing the test will fail and if available
 * the records of the expression will be printed as a single line to
 * stdout.
 * 
 * NOTE: lines can be extended across lines by using the \ symbol as the last char of the line 
 *
 */
public class CommentExpressionsTester {
    private final static Pattern expressionPattern = Pattern.compile("^E\\s+(?<exp>.*)");
    private final static Pattern responsePattern = Pattern.compile("^R\\s+(?<resp>.*)");
    private final static Pattern numSunkPattern = Pattern.compile("^numSunk\\s+(?<symb>[\\>=\\<]+)\\s+(?<num>\\d+)");
    private final JackKnife knife;
    private final Iterator<String> lines;
    
    public CommentExpressionsTester(JackKnife knife, Object testClass) throws IOException {
        this.knife = knife;
        String s = testClass.getClass().getCanonicalName();
        String testClassJavaFilePath = "tst/" + s.replace('.', '/') + ".java";
        List<String> lines = FileUtils.readLines(new File(testClassJavaFilePath));
        
        // concatentate long lines that use the \ symbol at the end
        int i = 0;
        while (i < lines.size() - 1) {
            String line = lines.get(i).trim(); // even allow white space after the \
            if (line.endsWith("\\")) {
                lines.set(i, line.substring(0, line.length()-1) + lines.get(i+1));
                lines.remove(i+1);
            }
            
            else i++;
        }
        
        this.lines = lines.iterator();
    }
    
    public ExpressionTest next() throws IOException, SyntaxError {
        while (lines.hasNext()) {
            String line = lines.next();
            if (line == null) return null;

            Matcher m = expressionPattern.matcher(line);
            if (!m.find()) continue;
            String exp = m.group("exp");
            
            // is the response a numSunk ?
            line = lines.hasNext() ? lines.next() : "";  
            m = numSunkPattern.matcher(line);            
            if (m.find()) {
                String s = m.group("num");
                int numExpected = Integer.parseInt(s);
                String compareSymbol = m.group("symb");
                return new ExpressionNumSunkTest(knife, exp, compareSymbol, numExpected);
            }            
            
            // else next line should be response
            m = responsePattern.matcher(line);
            String resp = m.find() ? m.group("resp") : null;
            
            return new ExpressionResponseTest(knife, exp, resp);                
        }
        
        return null;
    }
}
