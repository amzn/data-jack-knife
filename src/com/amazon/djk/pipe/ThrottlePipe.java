package com.amazon.djk.pipe;


import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.amazon.djk.report.ScalarResolver.AggType.NONE;

/**
 * Pipe for throttling the speed of the records being processed with the TPS
 * configuration.
 */
@ReportFormats(headerFormat="TPS=%,d")
public class ThrottlePipe extends RecordPipe {
    private static final int MICROSECONDS_IN_SECOND = 1000 * 1000;
    private final OpArgs args;

    @ScalarProgress(name="TPS", aggregate = NONE)
    private final int tps;

    /**
     * Number of processed records in the current batch.
     */
    private int numRecordsProcessed = 0;

    /**
     * Start time in microseconds of the current batch.
     */
    private long batchStartTime = 0;

    public ThrottlePipe(OpArgs args) throws IOException {
        this(null, args);
    }

    public ThrottlePipe(ThrottlePipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        this.tps = (Integer) args.getArg("TPS");
    }

    @Override
    public Record next() throws IOException {
        // check whether we have processed enough records for a batch
        if (numRecordsProcessed == getRecordNumberPerBatch()) {
            long elapsedTime = getElapsedTimeForCurrentBatch();
            long expectTime = getExpectedTimeForBatch();
            if (expectTime > elapsedTime) {
                try {
                    TimeUnit.MICROSECONDS.sleep(expectTime - elapsedTime);
                } catch (InterruptedException e) {}
            }
            numRecordsProcessed = 0;
        }
        // if numRecordsProcessed is 0, start with another batch
        if (numRecordsProcessed == 0) {
            batchStartTime = System.nanoTime() / 1000;
        }
        numRecordsProcessed ++;
        return super.next();
    }

    /**
     * Use the TPS per thread to determine the batch size, the batch size should be at least 1.
     */
    private int getRecordNumberPerBatch() {
        return Math.max(tps / getNumActiveInstances(), 1);
    }

    private long getElapsedTimeForCurrentBatch() {
        return System.nanoTime() / 1000 - batchStartTime;
    }

    /**
     * The expected time for batch equals recordNumberPerBatch / expectedTpsPerThread.
     */
    private int getExpectedTimeForBatch() {
        return MICROSECONDS_IN_SECOND * getRecordNumberPerBatch() * getNumActiveInstances() / tps;
    }

    @Override
    public Object replicate() throws IOException {
        return new ThrottlePipe(this, args);
    }

    @Description(text = {"throttles the record processing with the specified TPS across threads"})
    @Arg(name = "TPS", gloss = "desired TPS across the threads", type = ArgType.INTEGER)
    @Example(expr = "numSinkThread=1; blanks:100 throttle:100 devnull", type= ExampleType.EXECUTABLE_GRAPHED)
    public static class Op extends PipeOperator {
        public Op() {
            super("throttle:TPS");
        }

        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException {
            return new ThrottlePipe(args).addSource(operands.pop());
        }
    }
}
