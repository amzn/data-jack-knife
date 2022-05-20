package com.amazon.djk.source;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.core.BaseRecordSource;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.core.Splittable;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;

@ReportFormats(headerFormat="<number>%d")
public class BlanksSource extends BaseRecordSource implements Splittable {
	public final static String FORMAT = "blanks";
	private final Record rec = new Record();
	private AtomicInteger numInstances;
	private final int instanceNo;
	@ScalarProgress(name="number", aggregate=AggType.NONE)
	private long limit;
	private long numRecs = -1;

	public BlanksSource(OpArgs args) throws IOException {
        this(new AtomicInteger(0), (Long)args.getArg("N"));
	    reportTotalRecords(limit);
	}
	
    public BlanksSource(AtomicInteger numInstances, long limit) {
        this.numInstances = numInstances;
        this.instanceNo = numInstances.getAndIncrement();
        this.limit = limit;
    }
    
    public long getLimit() {
        return limit;
    }
    
    /**
     * constructor for empty source
     */
    public BlanksSource() {
        this.instanceNo = 0;
        this.limit = 0;
    }
	
	@Override
	public Object split() {
		return new BlanksSource(numInstances, limit);
	}

    @Override
    public Record next() throws IOException {
        while (++numRecs < limit || limit == -1) {
            if (numRecs % numInstances.get() == instanceNo) {
                rec.reset();
                reportSourcedRecord(rec);
                return rec;
            }
        }
        
    	return null;
    }
    
	@Description(text={"A source of empty records."})
	@Arg(name = "N", gloss = "Number of blank records to produce. -1 means infinite", type = ArgType.LONG)
    @Example(expr="blanks:5 devnull", type=ExampleType.EXECUTABLE_GRAPHED)
	public static class Op extends SourceOperator {

		public Op() {
			super(FORMAT + ":N", Type.USAGE);
		}

		@Override
		public RecordSource getSource(OpArgs args) throws IOException {
			return new BlanksSource(args);
		}
    }
}
