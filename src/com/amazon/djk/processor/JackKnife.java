package com.amazon.djk.processor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.ChunkTokenizer;
import com.amazon.djk.expression.Expression;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.OperatorManual;
import com.amazon.djk.manual.UsageManual;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ProgressReport;

/**
 * 
 *
 */
public class JackKnife extends InnerKnife {
	
	public JackKnife() throws DJKInitializationException {
		super();
	}

	/**
	 * 
	 * @param value
	 * @return
	 */
    public JackKnife setReportOnce(boolean value) {
        reportOnce = value;
        return this;
    }
    
    /**
     * primarily for testing
     * 
     * @param expression
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    public List<Record> collectMain(String expression) throws IOException, SyntaxError {
    	return collectMain(null, expression);
    }
    
    /**
     * primarily for testing
     * 
     * @param upstream
     * @param expression
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    public List<Record> collectMain(RecordSource upstream, String expression) throws IOException, SyntaxError {
    	CollectionContext context = CollectionContext.create(this, upstream, Expression.create(expression));
    	CollectionResults results = collect(context);
    	return results.getMainResults();
    }
    
    public CollectionResults collect(String expression) throws IOException, SyntaxError {
    	return collect(Expression.create(expression));
    }
    
    public ProgressReport execute(String expression) throws SyntaxError, IOException {
    	return execute(Expression.create(expression));
    }
    
    public ProgressReport execute(RecordSource upstream, String expression) throws SyntaxError, IOException {
    	return execute(upstream, Expression.create(expression));
    }
    
    /**
     * the java main entry point.
     * 
     * @param args
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    public ProgressReport executeMain(String[] args) throws IOException, SyntaxError {
		final String CMD_FILE = "cmdFile:";
		if (args.length == 1 && args[0].startsWith(CMD_FILE)) {
			args = getFileCmd(args[0].substring(CMD_FILE.length()));
		}

    	if (args.length == 2 && args[0].equals("man")) {
    	    OperatorManual manual = new OperatorManual(this);
    	    manual.addTopic(args[1]);
            manual.display();
    		return null;
   	 	}
    	
    	if (args.length == 1 && args[0].equals("emacs")) {
    		EmacsModeWriter emacsMode = new EmacsModeWriter(getParser());
    		emacsMode.create();
    		return null;
    	}

    	Expression expr = Expression.create(args);

    	try {
    		return execute(expr);
    	}
    	
    	catch (SyntaxError error) {
   		 	OperatorManual manual = new OperatorManual(this);
   		 	manual.addSyntaxError(error, expr.getAllChunks().toArray());
            manual.display();
   		 	throw error;
    	}
    }

    /**
     * 
     * @param file
     * @return
     * @throws IOException 
     */
    private static String[] getFileCmd(String file) throws IOException {
    	List<String> lines = FileUtils.readLines(new File(file));
    	for (String line : lines) {
    		if (line.startsWith("#")) continue;
    		return ChunkTokenizer.split(line);
    	}
           
    	return new String[0];
    }
    
    
   protected void printUsage() throws IOException {
    	UsageManual manual = new UsageManual(this);
    	manual.display();
    }
    
    /**
     * 
     * @param clazz with the same classpath as the resource
     * @param resourceName of the resource
     * @return the resource as lines
     * @throws IOException 
     */
	public static List<String> getResourceAsLines(Object clazz, String resourceName) throws IOException {
        InputStream stream = clazz.getClass().getResourceAsStream(resourceName);
        if (stream == null) {
            throw new IOException("resource '" + resourceName + "' not found");
        }
        InputStreamReader isr = new InputStreamReader(stream);
        BufferedReader reader = new BufferedReader(isr);
        List<String> lines = new ArrayList<>();
		while (true) {
			String line = reader.readLine();
			if (line == null) break;
			lines.add(line);
		}
		
		return lines;
    }
}
