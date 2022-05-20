package com.amazon.djk.processor;

import java.io.IOException;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Display.DisplayType;
import com.amazon.djk.pipe.ForeachPipe;
import com.amazon.djk.pipe.IfPipe;
import com.amazon.djk.report.DisplayMode;

/**
 * NOTE: generalized from a MapSource loader to generic InnerSinkerDrainer.
 * 
 * MapSources are loaded with syntactic sugar from StreamSources, e.g.
 * 
 * djk cars.nvp?keys=id,color
 * 
 * This wrapper class makes it possible to have the framework MULTITHREAD
 * the loading of the map BEFORE the main expression runs.  Otherwise the
 * loading has to happen within Operator.getAsPipe() method single-threadedly.
 * 
 * The RecordProcessor looks for DrainableMapSources BEFORE calling getStrand()
 * on the mainSink and multi-threading the main execution.
 *  
 */
public class InnerSinkDrainer {
    
    public static void drain(InnerKnife processor, RecordSink sink) throws IOException, SyntaxError {
        innerDrain(processor, sink);
    }

	/**
	 * recurse the execution graph looking for WithInnerSinks to drain
	 * 
	 * recurse WithInnerSink.source before draining WithInnerSink
	 * recurse Join.right before Join.left
	 * recurse depth first-search
	 * 
	 * traverse into:
	 * 
	 * IfPipe.trueExpression
	 * IfPipe.falseExpression
	 * subExpressions
	 * JoinPipe.right
	 * JoinPipe.left
     * 
     * @param processor
     * @param incomingExpression
     * @param sinkReport the report to which all discovered inner sink reports are added as intermediate reports
     * @throws IOException
     * @throws SyntaxError
     */
	private static void innerDrain(InnerKnife processor, RecordSource incomingExpression) throws IOException, SyntaxError {
		RecordSource source = incomingExpression;

		while (true) {
			if (source instanceof WithInnerSink) {
				WithInnerSink withInner = (WithInnerSink)source;
				
				DisplayType displayType = CoreDefs.get().getDisplayType();
				DisplayMode mode = displayType == DisplayType.DEFAULT ?
				        DisplayMode.SHOW :
				        displayType == DisplayType.VT100 ?
				        DisplayMode.SHOW : DisplayMode.NO_SHOW;
				
				RecordSink innerSink = withInner.getSink();

                // now load our bad self
				processor.threadedExecute(innerSink, mode);
				withInner.finishSinking(processor);
			}
			
			/**
			 * These should implement an interface that allows them to be traversed
			 * without having to enumerate them.
			 * 
			 * List<RecordSource> WithOtherSources.getOtherSources()
			 */
			
			else if (source instanceof WithKeyedSource) {
			    WithKeyedSource wks = (WithKeyedSource)source;
			    innerDrain(processor, wks.getKeyedSource());
			}
			
			else if (source instanceof IfPipe) {
				RecordPipe trueClause = ((IfPipe)source).getTrueClause();
				innerDrain(processor, trueClause);
				
			    RecordPipe falseClause = ((IfPipe)source).getFalseClause(); 
			    if (falseClause != null) {
			    	innerDrain(processor, falseClause);
			    }
			}
			
			else if (source instanceof ForeachPipe) {
				RecordPipe subexp = ((ForeachPipe)source).getSubExpression();
				innerDrain(processor, subexp);
			}
			
			if (source instanceof RecordPipe) {
				source = ((RecordPipe) source).getSource();
				continue;
			}

			// else strict RecordSource, we're done
			return;
		}
	}
}
