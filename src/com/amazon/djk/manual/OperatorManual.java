package com.amazon.djk.manual;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.ExpressionParser;
import com.amazon.djk.expression.InternalMacroOperator;
import com.amazon.djk.expression.Operator;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.file.FileOperator;
import com.amazon.djk.file.FileSystem;
import com.amazon.djk.format.FormatOperator;
import com.amazon.djk.format.FormatParser;
import com.amazon.djk.format.FormatWriter;
import com.amazon.djk.format.WriterOperator;
import com.amazon.djk.keyed.KeyedSource;
import com.amazon.djk.processor.JackKnife;
import com.amazon.djk.processor.MacroPipe;
import com.amazon.djk.reducer.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class OperatorManual extends Manual {
	private final GlossesSection glossesSection;
	private final ExamplesSection exampleSection;
	private final SyntaxSection syntaxSection;
	private final JackKnife knife;
	
	public OperatorManual(JackKnife knife) throws IOException {
		this.knife = knife;
		glossesSection = new GlossesSection(this);
		exampleSection = new ExamplesSection(this.knife, this);
		syntaxSection = new SyntaxSection(this);
	}
	
	/**
	 * 
	 * @param op
	 * @return true if able to display using Operators annotations
	 * @throws IOException 
	 */
	private boolean addOperator(Operator op) throws IOException {
        Description description = null;
        try {
            description = op.getClass().getAnnotation(Description.class);
            if (description == null) return false;
        }
        
        catch (SecurityException | IllegalArgumentException e) { 
            return false;
        }
		
		addDescription(op, description);
		if (! (op instanceof ManPage)) {
			glossesSection.display(op);
		}
        addPreLines(description.preLines());
		exampleSection.display(op);

		String packageFQCN = getBestEffortPackageAndFQCN(op);
		if (packageFQCN != null) {
			addLine();
			addString("code:", lightblue());
			addLine(" {" + packageFQCN + "}");
			addLine();
		}

		return true;
	}

	/**
	 * best effort package determination
	 *
	 * @param op
	 * @throws IOException
	 */
	private String getBestEffortPackageAndFQCN(Operator op) throws IOException {
		Class clazz = op.getClass();
		Class eclazz = clazz.getEnclosingClass();
		if (eclazz == null) return null;

		String FQCN = eclazz.getName();
		String className = eclazz.getSimpleName() + ".class";
		String classPath = eclazz.getResource(className).toString();
		String packageName = null;

		/**
		 * when the classPath is within a jar file
		 */
		// http://www.rexegg.com/regex-lookarounds.html
		// jar:file:/tmp/DataJackKnife-1.0.jar!/com/amazon/djk/pipe/IfPipe.class
		// jar:file:/Volumes/brazil-pkg-cache/packages/DJKAmazonCommons/DJKAmazonCommons-2.0.201601.0/AL2012/DEV.STD.PTHREAD/build/lib/DJKAmazonCommons-2.0.jar!/com/amazon/djk/s3/S3FileSystem.class
		Pattern jarClasspathPat = Pattern.compile("(?<pack>[^\\/\\-\\.]+)[\\d\\-\\.]*(?=\\.jar\\!)");
		if (classPath.startsWith("jar:")) {
			Matcher m = jarClasspathPat.matcher(classPath);
			if (m.find()) {
				packageName = m.group("pack");
			}
		}

		//classPath = file:/Volumes/workplace/djkrest/out/production/DataJackKnife/com/amazon/djk/sink/DevnullSink.class
		else if (classPath.startsWith("file:")){
			/**
			 * when class is coming from a directory, hack, hack, hack
			 */
			String src = FQCN.replaceAll("\\.", "\\/");
			int pos = classPath.indexOf(src);
			if (pos == -1) return null;
			packageName = classPath.substring(0, pos - 1);
			pos = packageName.lastIndexOf('/');
			if (pos == -1) return null;
			packageName = packageName.substring(pos + 1);
		}

		return packageName != null ? String.format("%s/%s", packageName, FQCN) : null;
	}

	/**
	 * 
	 * @param op
	 * @param description
	 */
	private void addDescription(Operator op, Description description) {
        Class<?> predicateClass = op.getMostSpecificPredicateType();

    	String color = blue();
    	String operatorType = null;
    			
        if (predicateClass != null) {
        	operatorType = predicateClass.getSimpleName();
        	if (predicateClass == RecordSink.class) {
        		color = red();
        	} else if (predicateClass == FormatParser.class){
        		color = green();
        	} else if (predicateClass == FormatWriter.class){
        		color = red();        	
        	} else if (predicateClass == RecordSource.class ||
        			predicateClass == KeyedSource.class) {
        		color = green();
        	} else if (predicateClass == Reducer.class) {
        		color = magenta();
        	} else if (predicateClass == MacroPipe.class) {
        		color = bold();
        	} else if (predicateClass == RecordPipe.class) {
        		color = yellow();
        	} else if (predicateClass == FileSystem.class) {
        		color = green();
        	}
        }
	    
		addString(op.getName(), color);
		if (operatorType != null) {
			addString(" is a ");
			addString(operatorType + ".  ", color);
		}

		if (predicateClass == Reducer.class) {
            addLine("Within a subexpression, a Reducer's output record is joined to parent record;");
            addLine("in the main expression output records flow to the REDUCE[:instance,...] expression. ");
		} else if (predicateClass == FormatParser.class) {
			String regex = ((FileOperator) op).getStreamFileRegex();
			if (regex != null){
				addLine("Default file regex is \"" + regex + "\"");
			}
		} else if (predicateClass == FileSystem.class) {
			addLine("  Provides access to 'format' sources, e.g. 'nv2' and 'tsv' (see formats).");
		}
		
		else {
		    addLine();
		}
		
        addLine();
		for (String t : description.text()) {
			addLine(t);
		}
		addLine();
		
		if (! (op instanceof ManPage) ) {
			syntaxSection.display(op, description.contexts());
		}
	}
	
	private void addPreLines(String[] preLines) {
		if (preLines.length == 1 && preLines[0].length() == 0) return;
		
		for (String s : preLines) {
			addLine(s);
		}
		
		addLine();
	}
	
	/**
	 * 
	 * @param error
	 * @param cmd
	 */
	public void addSyntaxError(SyntaxError error, String[] cmd) {
		syntaxSection.addError(error, cmd);
	}
	
	/**
	 * 
	 * @param pipeOp
	 * @throws IOException 
	 */
	private void addPipeOp(PipeOperator pipeOp) throws IOException {
		if (addOperator(pipeOp)) return; // success displaying Operator annotations
	}
	
	/**
	 * would be nice to unify this with regular annotated operators 
	 * @param op
	 */
	private void addInternalMacro(InternalMacroOperator op) {
        addString(op.getName(), bold());
        addString(" is a ");
        addLine("Macro", bold());
        addLine();
        addLine(op.getDescription());
        addLine();
        addLine("syntax:", lightblue());
        addLine();
        addLine(op.getMacroUsage());
        addLine();
        addLine("this macro is equivalent to:", lightblue());
        addLine();
        List<String> raw = op.getRawMacroLines(null); // args unneeded for internal macros
        syntaxSection.displayExpAsTree(MacroPipe.getMacroString(raw));
        addLine();
        addLine();
	}
	
	/**
	 * 
	 * @param topic the subject to get a man page on
	 * @throws IOException 
	 */
	public void addTopic(String topic) throws IOException, SyntaxError {
		ExpressionParser parser = knife.getParser();
		
		Map<String,FileSystem> fsystems = parser.getFileSystems();
		FileSystem fs = fsystems.get(topic);
		if (fs != null) {
			addOperator(fs.getPathOperator());
			return;
		}
		
		PipeOperator pipeOp = parser.getPipeOps().get(topic);
		SourceOperator sourceOp = parser.getSourceOps().get(topic);
		
		FormatOperator formatOp = parser.getFormatOps().get(topic);
		if (formatOp != null) {
			addOperator(formatOp);
		}
		
		WriterOperator writerOp = parser.getWriterOps().get(topic);
		if (writerOp != null) {
			addOperator(writerOp);
		}

		ManPage manPage = knife.getManPage(topic);
		
		if (pipeOp instanceof InternalMacroOperator) {
		    addInternalMacro((InternalMacroOperator)pipeOp);
		}
		
		if (pipeOp == null && sourceOp == null && manPage == null && formatOp == null && writerOp == null) {
			addLine("unknown topic:" + topic, lightblue());
			return;
		}
		
		if (manPage != null) {
			addOperator(manPage);
		}
		
		if (pipeOp != null) {
			addPipeOp(pipeOp);
		}
		
		// could be both
		if (sourceOp != null) {
			addOperator(sourceOp);
		}
	}
}
