package com.amazon.djk.pipe;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;

import java.io.IOException;
import java.util.Random;

/**
 * if recno < nsamples; A[recno] = rec
 * else:
 *   i = p * recno
 *   if i < nsamples; A[i] = rec 
 */
public class RandomSamplePipe extends RecordPipe {

	private static final String SEED = "seed";
	private static final String DEFAULT_SEED = "-1";

	// unused
	public RandomSamplePipe() throws IOException {
        super(null);
    }

    @ReportFormats(headerFormat="numSamples=%d")
	public static class RandomSampleFixed extends RecordPipe {
	    protected final long seed;
	    protected final Random coin;
        private final boolean isRoot;
        
		@ScalarProgress(name="numSamples", aggregate=AggType.NONE)
		private final int nsamples;
	    private Record[] A = null;
	    private int numrecs = 0;
        private boolean ready = false;

        public RandomSampleFixed(int nsamples, long seed) throws IOException {
            this(null, nsamples, seed);
        }
		
		/*
		 * this is an approximation.  Since we can't no a priori how many records
		 * will flow throw each thread (BUT there is a STRONG likelihood that approx
		 * the same number will), we just divide the samples among the threads and
		 * call it good.
		 */
        public RandomSampleFixed(RandomSampleFixed root, int nsamples, long seed) throws IOException {
	    	super(root);
	        this.seed = seed;
	        this.isRoot = root == null;
	        coin = seed == Long.parseLong(DEFAULT_SEED) ? new Random() : new Random(seed);
	        
	        this.nsamples = nsamples;
	    }
	    
		@Override
	    public Object replicate() throws IOException {
			return new RandomSampleFixed(this, nsamples, seed);
	    }
		
		@Override
	    public Record next() throws IOException {
		    if (A == null) {
		        int childNum = (int)Math.floor((float)nsamples / (float)getNumInstances());
		        int rootNum = nsamples - (getNumInstances() - 1) * childNum; 
		        A = new Record[(isRoot ? rootNum : childNum)];
		    }
		    
	    	// exhaust input, collect samples
	    	long inrecno = 0; // becomes 1 based

	    	while (!ready) {
	    		inrecno++;
	    		
	    		Record rec = super.next();
	    		if (rec == null) {
	    			ready = true;
	    			break;
	    		}

	    		if (numrecs < A.length) {
	    			A[numrecs++] = rec.getCopy();
	    			continue;
	    		}

	    		else {
	                float p = coin.nextFloat(); // 0 - 1.0
	    			int i = (int)(p * inrecno);
	    			if (i < A.length) {
	    			    Record r = A[i];
	    			    r.reset();
	    			    r.addFields(rec);
	    			}
	    		}
	        }
	    	
	    	// at this point, samples collected, now output, do it backwards
	    	if (numrecs > 0) {
	    		int i = --numrecs;
	    		Record rec = A[i];
	    		A[i] = null;
	    		return rec;
	    	}
	    	
	    	ready = false; // reset ourselves
	    	A = null;
			return null;
	    }
	}
	
	/**
	 * 
	 */
	@ReportFormats(headerFormat="likelihood=%2.4f")
	public static class RandomSampleThresh extends RecordPipe {

		@ScalarProgress(name="likelihood", aggregate=AggType.NONE)
		private final float probThresh;
		protected final long seed;
        protected final Random coin;

        public RandomSampleThresh(float probThresh, long seed) throws IOException {
            this(null, probThresh, seed);
        }
        
        public RandomSampleThresh(RandomSampleThresh root, float probThresh, long seed) throws IOException {
	    	super(root);
	    	this.seed = seed;
            coin = seed == Long.parseLong(DEFAULT_SEED) ? new Random() : new Random(seed);
			this.probThresh = probThresh;
		}
		
		@Override
	    public Object replicate() throws IOException {
			return new RandomSampleThresh(probThresh, seed);
	    }
		
		@Override
		public Record next() throws IOException {
			while (true) {
				Record rec = super.next();
				if (rec == null) return null;
            
				float p = coin.nextFloat(); // 0 - 1.0
				if (p < probThresh) {
					return rec;
				}
			}
		}
    }

    public static PipeOperator getOperator() {
        return new Op();
    }
    
    @Description(text={"Provides a sample of the input records to its output"})
    @Arg(name="SPEC", gloss="<integer> | <float>", type=ArgType.STRING)
    @Gloss(entry="integer", def="results in exactly <integer> random records being chosen")
    @Gloss(entry="float", def="0.XY results in approx. XY% random records being chosen")
    @Param(name= SEED, gloss="seed for random generator", type=ArgType.LONG, defaultValue = DEFAULT_SEED)
    //@Example(expr="[ id:1 id:2 id:3 id:4 id:5 id:6 ] sample:2", type=ExampleType.DISPLAY_ONLY)
    @Example(expr="[ id:1 id:2 id:3 id:4 id:5 id:6 ] sample:0.5", type=ExampleType.EXECUTABLE)
    public static class Op extends PipeOperator {
        public Op() {
            super("sample:SPEC");
        }
    
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            String spec = (String)args.getArg("SPEC");
            float probThreshold = spec.indexOf('.') == -1 ? -1.0F : Float.parseFloat(spec);
            int nsamples = spec.indexOf('.') == -1 ? Integer.parseInt(spec) : -1;
            long seed = (Long)args.getParam(SEED);

            return nsamples == -1 ? new RandomSampleThresh(probThreshold, seed).addSource(operands.pop())
					: new RandomSampleFixed(nsamples, seed).addSource(operands.pop());
        }
    }
}
