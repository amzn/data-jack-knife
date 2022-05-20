package com.amazon.djk.manual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.CommaList;
import com.amazon.djk.expression.Expression;
import com.amazon.djk.expression.Operator;
import com.amazon.djk.processor.CollectionContext;
import com.amazon.djk.processor.CollectionResults;
import com.amazon.djk.processor.JackKnife;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.GraphDisplay;
import com.amazon.djk.report.ProgressReport;
import com.amazon.djk.sink.DevnullSink;
import com.amazon.djk.sink.PrintSink;
import com.amazon.djk.source.BlanksSource;

public class ExamplesSection {
	private final JackKnife core;
	private final Manual manual;
	
	public ExamplesSection(JackKnife core, Manual manual) {
		this.manual = manual;
		this.core = core;
	}

	/**
	 * to work around awkwardness of Examples and Example due to Annotations
	 * 
	 * @param op
	 * @param type
	 * @return
	 */
	private static List<Example> getExamples(Operator op, ExampleType type) {
		try {
			List<Example> examples = new ArrayList<>();
			
			Examples examplesObj = op.getClass().getAnnotation(Examples.class);
			if (examplesObj == null) { // look for individual
				Example e = op.getClass().getAnnotation(Example.class);
				if (e == null || e.type() != type) return null;
				
				examples.add(e);
				return examples;
			}
			
			else {
				for (Example e: examplesObj.value()) {
					if (e.type() == type) {
						examples.add(e);
					}
				}

				if (examples.size() == 0) return null;
				return examples;
			}
		}
		
		catch (SecurityException | IllegalArgumentException e) { }
		
		return null;
	}
	
	/**
	 * 
	 * @param op
	 */
	public void display(Operator op) throws IOException {
		List<Example> snippets = getExamples(op, ExampleType.DISPLAY_ONLY);
		List<Example> executables = getExamples(op, ExampleType.EXECUTABLE);		
		List<Example> graphed = getExamples(op, ExampleType.EXECUTABLE_GRAPHED);

		
		// plain executables then graphed
		if (executables == null) {
			executables = new ArrayList<>();
		}

		if (graphed != null) {
			executables.addAll(graphed);
		}

		if (snippets == null && executables.size() == 0) return;
		
		manual.addLine("examples:", manual.lightblue());
		manual.addLine();
		
		// just print the snippets
		if (snippets != null) {
			for (Example e : snippets) {
				// TODO: unify with SyntaxManual for determining where ... goes
				manual.addLine(e.expr(), manual.bold());				
			}
			manual.addLine();
		}
		
		int eNo = 1;
		for (Example x : executables) {
			ExampleType type = x.type();
			
			String expression = x.expr();
			manual.addLine("example " + eNo + "$ djk " + expression, manual.bold());
			
			Expression expr = Expression.create(expression);
			CollectionContext context = core.createCollectionContext(expr);
			verifyGraph(context);				
			CollectionResults results = core.collect(context);
			List<Record> recs = results.getMainResults();

			if (recs == null || expression.contains("devnull")){ // remove output if the expression contains devnull
				recs = new ArrayList<>();
			}

			List<Record> reductionOutput = results.getReduceResults(new CommaList("default"));
			if (reductionOutput != null && !reductionOutput.isEmpty()) {
				recs.addAll(reductionOutput);
			}

			for (Record rec : recs) {
				String[] recLines = rec.toString().split("\n");
				for (String line : recLines) {
					manual.addLine(line);
				}
				manual.addLine("#");
			}
				
			ProgressReport report = results.getReport();
				
			if (type == ExampleType.EXECUTABLE_GRAPHED) {
				manual.addLine();
				GraphDisplay display = new GraphDisplay();
				report.display(display); // or everything?
				String[] lines = display.getAsLines();
				for (String line : lines) {
					manual.addLine(line);	
				}
				manual.addLine();					
			} else {
				manual.addLine();
			}

			eNo++;
		}
	}
	
	/**
	 * ensure that the source of the expression meets requirements otherwise throw RuntimeException
	 * 
	 * @param context
	 * @throws IOException 
	 */
	private static void verifyGraph(CollectionContext context) throws IOException {
		RecordSink originalSink = context.getOriginalMainSink();
		
	    boolean isDevnull = originalSink instanceof DevnullSink;
	    if (! (originalSink instanceof PrintSink || isDevnull) ) {
            String s = originalSink.getClass().getSimpleName();
            throw new RuntimeException("@Example expression must omit sink, or end with 'devnull'.  Currently ends with:" + s);
        }

	    RecordPipe pipe = originalSink;
	    RecordSource source = null;
	    while (true) {
	        source = pipe.getSource();
	        if (source instanceof RecordPipe) {
	            pipe = (RecordPipe)source;
	        }
	        
	        else break;
	    }
	   
	    if (source instanceof BlanksSource) {
	        long N = ((BlanksSource)source).getLimit();
	        if (N <= 1000 && originalSink instanceof DevnullSink) {
                return;
            }
	        
	        if (N <= 10 && originalSink instanceof PrintSink) {
	            return;
	        }

	        throw new RuntimeException("the @Example annotation '" + context.getExpression() + "' has too many blanks for printing.  You can send up to 1000 to devnull though.");
	    }
	}
}
