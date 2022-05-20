package com.amazon.djk.processor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import com.amazon.djk.expression.ExpressionParser;
import com.amazon.djk.expression.Operator;
import com.amazon.djk.expression.PipeOperator;
import com.google.common.collect.Sets;

/**
 * class for writing a major mode file for emacs. 
 *
 */
public class EmacsModeWriter {
	private final static String OUTPUT_SUBDIR = ".emacs.d/djk";
	private final static String MODE_FILE = "djk-mode.el";
	private final StringBuilder sb = new StringBuilder();
	// should automatically discover these by using an interface
	private final String[] keywordList = {"else", "if", "foreach", "join", "denorm", "acceptIf", "rejectIf"};
	private final Set<String> keywordSet;
	
	//private Map<String,PipeOperator> pipeOps;
	//private Map<String,SourceOperator> SourceOperators;
	private final ExpressionParser parser;
	public EmacsModeWriter(ExpressionParser parser) {
		this.parser = parser;
	    //this.pipeOps = parser.getPipeOps();
	    //this.SourceOperators = parser.getSourceFactory().getOperators();
	    keywordSet = Sets.newHashSet(keywordList);
	}
	
	public void create() throws IOException {
		String outputFormat = getOutputFormat();
		String keywords = getKeywords();
		String pipes = getPipes();
		String sources = getSources();
		
		// 3 %s's to resolve
		String output = String.format(outputFormat, keywords, pipes, sources);
		String userHome = System.getProperty("user.home");
		userHome = userHome == null ? "/tmp" :
			userHome.startsWith("/dev") ? "/tmp" : userHome;
		
		File homeDir = new File(userHome);
		File lispDir = new File(homeDir, OUTPUT_SUBDIR);
		FileUtils.forceMkdir(lispDir);
		File modeFile = new File(lispDir, MODE_FILE);
		FileUtils.writeStringToFile(modeFile, output);
		
		System.out.println("Wrote: " + modeFile.getAbsolutePath());
		System.out.println("add the following to ~/.emacs file:");
		System.out.println();
		System.out.println(";; setup files ending in “.djk” to open in djk-mode");
		System.out.println("(add-to-list 'load-path \"~/.emacs.d/djk/\")");
		System.out.println("(add-to-list 'auto-mode-alist '(\"\\.djk\\'\" . djk-mode))");
		System.out.println("(require 'djk-mode)");
	}

	/**
	 * 
	 * @return a list of sources (minus keywords, plus formats)
	 */
	private String getSources() {
		sb.setLength(0);
		
		Map<String,Operator> ops = parser.getAllOps();
		
		for (String e : ops.keySet()) {
			if (!keywordSet.contains(e)) {
				if (sb.length() != 0) {
					sb.append(' ');
				}

				sb.append('"');
				sb.append(e);
				sb.append('"');
			}
		}
		
		return sb.toString();
	}
	
	/**
	 * 
	 * @return a list of pipes (minus the 'keywords')
	 */
	private String getPipes() {
		sb.setLength(0);
		
		Map<String,PipeOperator> pops = parser.getPipeOps();
		
		for (Entry<String,PipeOperator> e : pops.entrySet()) {
			if ((e.getValue() instanceof PipeOperator) &&
				!keywordSet.contains(e.getKey())) {

				if (sb.length() != 0) {
					sb.append(' ');
				}

				sb.append('"');
				sb.append(e.getKey());
				sb.append('"');
			}
		}
		
		return sb.toString();
	}
	
	/**
	 * 
	 * @return the djk keywords
	 */
	private String getKeywords() {
		sb.setLength(0);
		for (String keyword : keywordList) {
			if (sb.length() != 0) {
				sb.append(' ');
			}

			sb.append('"');
			sb.append(keyword);
			sb.append('"');
		}
		
		return sb.toString();
	}
	
	/**
	 * 
	 * @return a string format with %s's for djk keywords, pipes, sources, etc.
	 * @throws IOException 
	 */
	private String getOutputFormat() throws IOException {
		List<String> lines = JackKnife.getResourceAsLines(this, "djk-mode.el");
		
		sb.setLength(0);
		for (String line : lines) {
			sb.append(line);
			sb.append('\n');
		}
		
		return sb.toString();
	}
}
