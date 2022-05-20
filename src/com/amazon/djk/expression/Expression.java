package com.amazon.djk.expression;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.pipe.FileBasedMacroOp;
import com.amazon.djk.processor.MacroPipe;

public class Expression {
	private static final Logger LOG = LoggerFactory.getLogger(Expression.class);
    public static final String MAIN_PART = "MAIN"; // comes first in chunks without PART prefix
    public static final String REDUCE_PART = "REDUCE";
    public static final String REPORT_PART = "REPORT";
    private final Map<String,ExpressionChunks> parts = new HashMap<>();
    private final List<JobProperty> properties = new ArrayList<>();
    private final Map<String, FieldDeclaration> fieldDeclarations = new HashMap<>();
    private final String[] allChunks;
    private boolean isResolved = false;

    /**
     * 
     * @param expression
     * @return
     * @throws IOException
     */
    public static Expression create(String expression) throws IOException {
        String[] chunks = ChunkTokenizer.split(expression);
    	return create(chunks);
    }
    
    /**
     * 
     * @param expression
     * @return
     * @throws SyntaxError
     * @throws IOException
     */
    public static Expression create(String[] expression) throws SyntaxError, IOException {
    	Expression e = new Expression(expression);
    	ExpressionChunks all = e.getAllChunks();
    	
    	// check to see if lone macro file
    	if (all.size() == 1 && all.get(0).startsWith(FileBasedMacroOp.MACRO + ':')) {
    		File f = new File(all.get(0).substring(FileBasedMacroOp.MACRO.length()+1));
    		if (f.exists()) {
    			return create(f, false);
    		}
    	}

    	return e;
    }
    
    /**
     * 
     * @param macroFile
     * @param keepTicks
     * @return
     * @throws IOException
     */
    public static Expression create(File macroFile, boolean keepTicks) throws IOException {
        List<String> rawLines = FileBasedMacroOp.getRawMacroLines(macroFile);
        String macroString = MacroPipe.getMacroString(rawLines);
        String[] chunks = ChunkTokenizer.split(macroString, keepTicks);
        return new Expression(chunks);
    }

    /**
     * InChunks may have come from the shell command line or
     * from the ChunkTokenizer.split(String).
     * Declarations may span chunks. (e.g. 'Property FILE = /tmp/foo.txt;')
     * 
     * @param inChunks
     * @throws IOException 
     */
    private Expression(String[] inChunks) throws IOException {
    	ExpressionChunks empty = new ExpressionChunks();
    	parts.put(MAIN_PART, empty);
    	parts.put(REPORT_PART, empty);
    	
    	ExpressionChunks xchunks = new ExpressionChunks();
    	xchunks.add(inChunks);
    	
    	xchunks = setFieldDeclarationsAndProperties(xchunks, true); // fields, job properties
        allChunks = xchunks.toArray(); // expression chunks are left over
        
        // the main expression (or part comes first without a part: chunk)
        ExpressionChunks partChunks = new ExpressionChunks();
        String partKey = MAIN_PART;
        
        
        for (String chunk : allChunks) {
        	String newKey = getMacroBoundary(chunk);
        	if (newKey != null) {
        		addPart(partKey, partChunks);
                partChunks = new ExpressionChunks();
                partKey = newKey;
        	}

        	else {
        		partChunks.add(chunk);
        	}
        }
        
        // the last macro
        if (!partChunks.isEmpty()) {
        	addPart(partKey, partChunks);
        }
    }
    
    /**
     * add the part chunks to the map under the part key
     * 
     * @param partKey
     * @param macroChunks
     */
    private void addPart(String partKey, ExpressionChunks macroChunks) {
        parts.put(partKey, macroChunks.isEmpty() ? null : macroChunks);
    }
    
    /**
     * 
     * @return
     */
    public List<JobProperty> getJobProperties() {
        return properties;
    }
    
    public Collection<FieldDeclaration> getFieldDeclarations() {
        return fieldDeclarations.values();
    }
    
    /**
     * 
     * @return all chunks of the input, i.e. MAIN, REDUCE, REPORT
     */
    public ExpressionChunks getAllChunks() {
    	return new ExpressionChunks(allChunks);
    }
    
    public ExpressionChunks getMainChunks() {
        return parts.get(MAIN_PART);
    }
    
    public Map<String,ExpressionChunks> getParts() {
    	return parts;
    }
    
    /**
     * map of [<instance>] --> expression
     */
    public Map<CommaList,ExpressionChunks> getReduceExpressionChunks() {
    	Map<CommaList,ExpressionChunks> reduceMacros = new HashMap<>();
    	Set<String> keys = parts.keySet();
    	
    	final String PREFIX = "REDUCE:";
    	for (String key : keys) {
    		if (key.startsWith(PREFIX)) {
    			CommaList instances = new CommaList(key.substring(PREFIX.length()));
    			reduceMacros.put(instances, parts.get(key));
    		}
    	}
    	
        return reduceMacros;
    }
    
    public ExpressionChunks getReportChunks() {
        return parts.get(REPORT_PART);
    }
    
    /**
     * 
     * @return a version of the expression that will run at the command line without
     * requiring additional escaping.
     */
    public String getAsCommandLine() {
    	return getAsString(true);
    }
    
    private String getAsString(boolean withEscaping) {
    	StringBuilder sb = new StringBuilder();
    	for (JobProperty prop : properties) {
    		sb.append(prop);
    		sb.append(withEscaping ? "\\; " : "; ");
    	}
    	
    	for (FieldDeclaration dec : getFieldDeclarations()) {
    		sb.append(dec);
    		sb.append(withEscaping ? "\\; " : "; ");
    	}
    	
    	if (sb.length() != 0) {
    		sb.append(' ');
    	}
    	
    	ExpressionChunks list = getMainChunks();
    	sb.append(list.getAsString());
    	
    	// map [instance] --> expression
    	Map<CommaList,ExpressionChunks> reduceMacros = getReduceExpressionChunks();
    	if (reduceMacros != null) {
    		Set<CommaList> instancesList = reduceMacros.keySet();
    		for (CommaList instances : instancesList) {
        		sb.append(' ');
    			sb.append("REDUCE:");
    			sb.append(instances);
    			sb.append(' ');
    			list = reduceMacros.get(instances);
    			sb.append(list.getAsString());
    		}
    	}
    	
    	list = getReportChunks();
    	if (list != null && !list.isEmpty()) {
    		sb.append(" REPORT ");
    		sb.append(list.getAsString());
    	}

    	return sb.toString();
    }
    
    /**
     * Sets the field declarations and properties
     * 
     * @param input the entire chunked expression
     * @param overrideFieldDeclarations boolean to override field declarations
     * @return expressionChunks with field declarations and properties removed
     * @throws SyntaxError
     */
    public ExpressionChunks setFieldDeclarationsAndProperties(ExpressionChunks input, boolean overrideFieldDeclarations) throws SyntaxError {
		StringBuilder sb = new StringBuilder();
		do { } while (setInner(input, sb, overrideFieldDeclarations));
		return input;
	}
	
	/**
	 * An arduous search for declarations.  We don't know how many chunks a declaration spans.
	 * Use regex here to find declarations within a string constructed from chunks.
     * removes and returns chunks that contain declarations of various types:
     * System Property : FOO = bar;
     * 					 GOO = ${FOO};
     * Field Declaration: STRING title;
     * Field Definition: LONG id = 1;
     * Local Field Definition: local DOUBLE height = 3.4;
     * 
	 * @param input
	 * @param overrideFieldDeclarations  boolean to specify whether field declarations can be overriden or not.
	 * @param sb
	 * @return
	 * @throws SyntaxError
	 */
	private boolean setInner(ExpressionChunks input, StringBuilder sb, boolean overrideFieldDeclarations) throws SyntaxError {
		sb.setLength(0); // clear
		int numChunks = 0;
		
		for (String chunk : input) {
			if (sb.length() != 0) {
				sb.append(" ");
			}
			sb.append(chunk);
			numChunks++;
			
			JobProperty prop = JobProperty.create(sb.toString());
			if (prop != null) {
				properties.add(prop);
				removeFrontChunks(input, numChunks);
				return true;
			}

			FieldDeclaration dec = FieldDeclaration.create(sb.toString().trim());
			if (dec != null) {
				if (overrideFieldDeclarations) {
					fieldDeclarations.put(dec.field, dec);
				} else {
					fieldDeclarations.putIfAbsent(dec.field, dec);
				}
				removeFrontChunks(input, numChunks);
				return true;
			}
		}
		
		return false;
	}
	
	private static void removeFrontChunks(ExpressionChunks chunks, int numChunks) {
		for (int i = 0; i < numChunks; i++) {
			chunks.remove(0);
		}
	}
	
	 /**
     * 
     * @param chunk
     * @return the macroKey for this boundary
     */
    private String getMacroBoundary(String chunk) {
    	String[] boundaries = {"REPORT", "REDUCE"};
    	
    	for (int i = 0; i < boundaries.length; i++) {
    		if (chunk.startsWith(boundaries[i])) {
    			
    			if (chunk.equals("REDUCE")) { // i.e. default reduce with no instance names
    				return "REDUCE:default";
    			} else {
    				return chunk;
    			}
    		}
    	}

    	return null;
    }
    
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder();
    	sb.append("MAIN=");
    	sb.append(parts.get(MAIN_PART));
    	sb.append("\n");
    	
    	sb.append("REDUCE=");
    	sb.append(getReduceExpressionChunks());
    	sb.append("\n");
    	
    	sb.append("REPORT=");
    	sb.append(parts.get(REPORT_PART));
    	sb.append("\n");

    	sb.append("FIELD_DECLARATIONS=");
    	sb.append(fieldDeclarations.toString());
    	sb.append("\n");
    	
    	sb.append("PROPERTIES=");
    	sb.append(properties.toString());
    	sb.append("\n");
    	
    	return sb.toString();
    }
    

}
