package com.amazon.djk.report;

import java.io.IOException;

import com.amazon.djk.core.Denormalizer;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.core.Denormalizer.AddMode;
import com.amazon.djk.core.Denormalizer.Context;
import com.amazon.djk.expression.Expression;
import com.amazon.djk.expression.ExpressionChunks;
import com.amazon.djk.processor.ExecutionContext;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Record;

/**
 * Report rooted at the ExecutionContext 
 *
 */
public class ProgressReport extends NodeReport {
    public ProgressReport(ReportProvider rootProvider, String nodeName) {
        super(rootProvider, nodeName);
    }

    public ProgressReport(ReportProvider rootProvider) {
        super(rootProvider);
    }

    /**
     * 
     * @return the hierarchical execution graph as a single record
     * @throws IOException
     */
    public Record getAsRecord(boolean flatten) throws IOException {
        Record main = new Record();
        addNodeTo(main);

        if (!flatten) return main;
        Record flattened = new Record();
        flatten(main, flattened, -1, 0);
        return flattened;
    }
    
    private static final String NODE_LEVEL = "nodeLevel";
    private static final String NODE_NO = "nodeNo";
    private void flatten(Record input, Record flat, int nodeLevel, int nodeNum) throws IOException {
    	Field field = new Field("node");
    	field.init(input);
    	if (!field.next()) {
    		input.addField(NODE_LEVEL, nodeLevel);
    		input.addField(NODE_NO, nodeNum);
    		flat.addField("node", input);
    		return; // bottom out recursion
    	}
    	
    	Denormalizer denormer = new Denormalizer("node", AddMode.CHILD_FIELDS_ONLY, Context.SIMPLE_DENORM);;
    	Record remnant = denormer.init(input);
    	if (remnant.length() > 0) {
    		remnant.addField(NODE_LEVEL, nodeLevel);
    		remnant.addField(NODE_NO, nodeNum++);
    		flat.addField("node", remnant);
    	}
		
    	nodeNum = 0; // change in level
    	while (true) {
    		Record node = denormer.next();
    		if (node == null) break;
    		flatten(node, flat, nodeLevel+1, nodeNum++);
    	}
    }
    
    public long numMainRecsSunk() {
    	return (rootProvider instanceof ExecutionContext) ?
        		((ExecutionContext)rootProvider).getMainNumSunk() : -1;	
    }
    
    public ExpressionChunks getAllExpressionChunks() {
    	if ( !(rootProvider instanceof ExecutionContext) ) {
    		return null;
    	}

    	Expression e = ((ExecutionContext)rootProvider).getExpression();
    	return e.getAllChunks();
    }
    
    /**
     * 
     * @param flatten should the record be flattened
     * @return
     * @throws IOException
     */
    public RecordSource getAsRecordSource(boolean flatten) throws IOException {
        Record main = getAsRecord(flatten);
        return RecordSource.singleton(main);
    }

    /**
     * 
     * @param display
     * @param mode
     */
    public void display(ReportDisplay display) {
        display.appendFrom(this);
    }
}
