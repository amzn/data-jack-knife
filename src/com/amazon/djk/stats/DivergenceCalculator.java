package com.amazon.djk.stats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.amazon.djk.core.Denormalizer;
import com.amazon.djk.core.Denormalizer.AddMode;
import com.amazon.djk.core.Denormalizer.Context;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Record;

/**
 * 
 *
 */
public class DivergenceCalculator {
	private final static String TYPE_FIELD = "type";
	private final static String POINT_FIELD = "point";
    private final Denormalizer denormer;
    private Map<String,Double> leftMap = new HashMap<>();
    private final Field field;
    private Record out = null;
    private double crossEntropy = 0.0;
    private String type = null;
    
    private Record lstatsOf = null;
    
    /**
     * main constructor 
     * @param root
     * @param args
     * @throws IOException
     */
    public DivergenceCalculator(Field field) throws IOException {
    	this.field = field;
        denormer = new Denormalizer(POINT_FIELD, AddMode.CHILD_FIELDS_ONLY, Context.SIMPLE_DENORM);
    }
    
    /**
     * 
     * @param rec
     * @return true if the record was processed
     * @throws IOException
     */
    public void offer(Record rec) throws IOException {
        if (out != null) return; // received 2 recs
        
        rec.deleteAll(DivergenceReducer.FIELD_FIELD); // not needed in child
    	String intype = rec.getFirstAsString(TYPE_FIELD);
        rec.deleteAll(TYPE_FIELD); // not needed in child
        
    	if (type != null && !type.equals(intype)) {
    		throw new SyntaxError("types not equal");
    	}
    	type = intype;
    	
        if (lstatsOf != null) {
        	 Record leftChild = processStatsOf(lstatsOf, null); // adds to leftMap
             Record rightChild = processStatsOf(rec, leftChild); // adds to leftMap

             out = new Record();
             out.addField(DivergenceReducer.FIELD_FIELD, field.getName());
             out.addField(TYPE_FIELD, type);
             out.addField("crossEntropy", crossEntropy);
             out.addField("KLDivergence", crossEntropy - leftChild.getFirstAsDouble("entropy"));
             out.addField("left", leftChild);
             out.addField("right", rightChild);
        }
        
        lstatsOf = rec.getCopy(); // records from next() have no guarantees
    }
    
    public Record getResult() throws IOException {
        return out;
    }

    /**
     * 
     * @param inrec
     * @return the entropy of stat1
     * @throws IOException
     */
    private Record processStatsOf(Record inrec, Record leftChild) throws IOException {
    	Record outStats = new Record();

    	Record parent = denormer.init(inrec); // parent = all but points
    	outStats.addFields(parent);
    	
    	long numPoints = parent.getFirstAsLong("numValues");
        double entropy = 0.0;
        crossEntropy = 0.0;
        
        while (true) {
            Record point = denormer.next();
            if (point == null) break;

            String value = point.getFirstAsString("value");
            Long count = point.getFirstAsLong("count");
            double prob = (double)count / (double)numPoints;
            entropy += prob * log2(prob);
            
            if (leftChild == null) { // i.e. we are computing leftChild
            	leftMap.put(value, prob);
            } else {
                Double prob1 = leftMap.get(value);
                if (prob1 == null) {
                	prob1 = 1.0 / (double)leftChild.getFirstAsLong("numValues");
                }
                
                crossEntropy += prob * log2(prob1);
            }
        }

        entropy *= -1;
        crossEntropy *= -1;
        
        outStats.addField("entropy", entropy);
        return outStats;
    }
    
    private double log2(double value) {
    	return Math.log(value) / Math.log(2);
    }
}
