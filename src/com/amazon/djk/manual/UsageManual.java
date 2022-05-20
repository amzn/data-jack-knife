package com.amazon.djk.manual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.expression.ExpressionParser;
import com.amazon.djk.expression.InternalMacroOperator;
import com.amazon.djk.expression.Operator;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.file.FileOperator;
import com.amazon.djk.file.FileSystem;
import com.amazon.djk.format.FormatOperator;
import com.amazon.djk.keyed.KeyedSink;
import com.amazon.djk.processor.JackKnife;
import com.amazon.djk.reducer.Reducer;
import com.amazon.djk.processor.MacroPipe;

public class UsageManual extends Manual {
    private static final int DESC_LEN = 50;
    private static String DEFAULT_FORMAT = "%-20.20s\t%-" + DESC_LEN + '.' + DESC_LEN + "s";
	private final ExpressionParser parser;
	private final Map<String,ManPage> miscManPages;
	
	public UsageManual(JackKnife knife) throws IOException {
		parser = knife.getParser();
		miscManPages = knife.getMiscManPages();
	    create();
	}
	
	/**
	 * print sources (but not keyword sources or formats)
	 * 
	 * @param strings
	 */
    private void printSourceList(String color) {
    	Map<String,SourceOperator> sops = parser.getSourceOps(); 
    	Set<String> sources = sops.keySet();

        for (String string : sources) {
            Operator op = sops.get(string);
            if (op.isKeyword()) continue;
            
            if (op instanceof FileOperator) {
                continue;
            }
            
            printOpDescription(op, color);
        }
        
        String s = String.format(DEFAULT_FORMAT, "PATH", "Path to 'scheme' 'format' files/directories");
        addLine(s);
    }
    
    /**
     * print the format list, e.g. nv2, txt, tsv, etc.
     * 
     * @param color
     */
    private void printFormatList(String color) {
    	Map<String,FormatOperator> fops = parser.getFormatOps(); 
    	Set<String> formats = fops.keySet();

        for (String string : formats) {
            Operator op = fops.get(string);
            if (op.isKeyword()) continue;
            
            if (! (op instanceof FileOperator) ) {
                continue;
            }
            
            printOpDescription(op, color);
        }
    }
    
    /**
     * FIXME: should have annotations of FileSystem (gloss?) to describe also fs relevant params
     * 
     * @param color
     */
    private void printFileSystemList(String color) {
        Map<String,FileSystem> fsystems = parser.getFileSystems();
        
        for (FileSystem fs : fsystems.values()) {
        	Operator op = fs.getPathOperator();
        	
        	Description description = op.getClass().getAnnotation(Description.class);
        	
        	String desc = " ";
            if (description != null) {
                String[] strs = description.text();
                desc = StringUtils.join(strs, " ");
            }
        	
            //String s = String.format("Provides access to 'format' files/directories via %s://PATH", fs.scheme());
            String s = String.format(DEFAULT_FORMAT, fs.scheme(), desc);
            if (getDisplayType() == DisplayType.HTML) {
            	s = String.format("man:%s:%s", color, s);
            }
            
            addLine(s);
        }
    }
    
    /**
     * print keywords
     */
    private void printKeywordList(String color) {
    	Map<String,Operator> ops = parser.getAllOps(); 
        Set<String> strings = ops.keySet();
        List<String> sorted = new ArrayList<>(strings);
        Collections.sort(sorted);
        
        for (String string : sorted) {
            Operator op = ops.get(string);
            if (!op.isKeyword()) continue;
            printOpDescription(op, color);
        }
    }
    
    
    /**
     * print pipes (but not keywords predicates)
     *  
     * @param strings
     * @param types
     */
    private void printPipeList(Class<?>[] types, String color) {
    	Map<String,PipeOperator> pops = parser.getPipeOps();
    	Set<String> pipes = pops.keySet();
    	
        for (String string : pipes) {
            Operator op = pops.get(string);
            if (op.isKeyword()) continue;
            
            Class<?> mostSpecificClass = op.getMostSpecificPredicateType();
            boolean isOne = false;
            for (Class<?> clazz : types) {
                if (mostSpecificClass == clazz) {
                    isOne = true;
                }
            }
            
            if (!isOne) continue; // skip;

            printOpDescription(op, color);
        }
    }
    
    private void printOpDescription(Operator op, String color) {
        Description description = op.getClass().getAnnotation(Description.class);
        String desc = " ";
        if (description != null) {
            String[] strs = description.text();
            desc = StringUtils.join(strs, " ");
        }
        
        if (op instanceof InternalMacroOperator) {
            desc = ((InternalMacroOperator)op).getDescription();
        }

        desc = Character.toUpperCase(desc.charAt(0)) + desc.substring(1);
        
        String s = String.format(DEFAULT_FORMAT, op.getName(), desc);
        if (getDisplayType() == DisplayType.HTML) {
        	s = String.format("man:%s:%s", color, s);
        }
        
        addLine(s);
    }
    
    private void printMiscList(String color) {
    	Set<String> topics = miscManPages.keySet();
    	
    	for (String topic : topics) {
    		ManPage manPage = miscManPages.get(topic);
    		printOpDescription(manPage, "black");
    	}

    	addLine();
    }
    
	/**
     * 
     * @param wrappingScriptName
     */
    private void create() {
        addLine();
        addString("keywords:", blue());
        addLine();
        printKeywordList("blue");
        addLine();
        
        addLine("sources:", green());
        printSourceList("green");
        addLine();
        
        addLine("formats:", green());
        printFormatList("green");
        addLine();
        
        addLine("filesystems:", green());
        printFileSystemList("green");
        addLine();
        
        addLine("pipes:", yellow());
        Class<?>[] types = new Class[]{RecordPipe.class};
        printPipeList(types, "yellow");
        addLine();
        
        addLine("reducers:", magenta());
        types = new Class[]{Reducer.class};
        printPipeList(types, "magenta");
        addLine();
        
        addLine("sinks:", red());
        types = new Class[]{RecordSink.class, KeyedSink.class};
        printPipeList(types, "red"); 
        addLine();
        
        addLine("macros:", bold());
        types = new Class[]{MacroPipe.class};
        printPipeList(types, "bold");
        addLine();
        
        addLine("misc:", bold());
        printMiscList("black");
        addLine();
    }
}
