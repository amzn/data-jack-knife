package com.amazon.djk.core;

import java.io.IOException;
import java.util.List;

import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;
import com.amazon.djk.report.PercentProgress;
import com.amazon.djk.report.RateProgress;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;

/**
 * 
 * base record source with reporting
 *
 */
@ReportFormats(lineFormats={
// FIXME: recs rate and MBRate also no workie
//        "recsRead=%,d (<RecsPercent>%2.1f%%) [<recsRate>%2.1f Krecs/sec/thread]",
        "recsRead=%,d (<RecsPercent>%2.1f%%) <MBread>%2.1f MB",
		//"<MBread>%2.1f MB read @ <MBrate>%2.2f MB/sec"
})
public class BaseRecordSource extends MinimalRecordSource {
	@PercentProgress(name="RecsPercent", denominatorAnnotation = "totalRecs")
	@RateProgress(name="recsRate") 
	@ScalarProgress(name="recsRead") // across all threads
	private volatile long numSourcedRecs = 0;

	@ScalarProgress(name="totalRecs", aggregate=AggType.NONE)
	private volatile long totalRecs = 0;
	
	@RateProgress(name="MBrate", multiplier=0.000001)
	@ScalarProgress(name="MBread", multiplier=0.000001)
    private volatile long bytesRead = 0;
		
    public long getNumRecsRead() {
        return numSourcedRecs;
    }
    
    public long bytesRead() {
        return bytesRead;
    }
    
    /**
     * increments numRecsRead and totalBytesRead for reporting purposes.
     *  
     * @param rec the record to be reported
     */
    public void reportSourcedRecord(Record rec) {
        numSourcedRecs++;
        bytesRead += rec.size();
    }   
    
    /**
     * reports the total number of records available in this source
     * @param totalRecs
     */
    public void reportTotalRecords(long totalRecs) {
        this.totalRecs = totalRecs;
    }
    
    /**
     * @return a record reflecting the origin of this source. Generally the record stored by
     * a ReportConsumer.
     */
    public Record getOriginReport() {
    	return null;
    }

	/**
	 * 
	 * @param record
	 * @return
	 * @throws IOException
	 */
	public static RecordSource getAsSource(List<Record> records) throws IOException {
		RecordFIFO fifo = new RecordFIFO();
		while (!records.isEmpty()) {
			fifo.add(records.remove(0));
		}
		
		return fifo;
	}
}
