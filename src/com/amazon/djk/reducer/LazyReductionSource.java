package com.amazon.djk.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.amazon.djk.core.BaseRecordSource;
import com.amazon.djk.core.MinimalRecordSource;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.core.Splittable;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ProgressData;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;

/**
 * This class is named lazy for emphasis only.  RecordSource.next() method is lazy by nature,
 * but it is important not to request records from the reducers at construction time
 * (like a RecordSource.singlton() does) since construction happens before the reducers have run.
 *
 */
@ReportFormats2(headerFormat="instances=%s")
public class LazyReductionSource extends BaseRecordSource implements Splittable {
	private final List<RecordSource> sources = new ArrayList<>();
	@ScalarProgress(name="instances")
	private String instancesDisplay;
	private final Set<String> instances = new HashSet<>();
	private RecordSource curr = null;
	
	/**
	 * Reduction comes from getNextMainReduction() method of the reducer
	 * 
	 * @param reducer
	 */
	public LazyReductionSource(Reducer reducer) {
		this.sources.add(new InnerReductionSource(reducer));
		this.instances.add(reducer.getInstanceName());
	}
	
	public LazyReductionSource(ReducerAggregator aggregator, String instanceName) {
		this.sources.add(aggregator);
		this.instances.add(instanceName);
	}
	
	public void add(Reducer reducer) {
		this.sources.add(new InnerReductionSource(reducer));
		this.instances.add(reducer.getInstanceName());
	}
	
	public void add(ReducerAggregator aggregator, String instanceName) {
		this.sources.add(aggregator);
		this.instances.add(instanceName);
	}
	
	public void add(LazyReductionSource lazySource) {
		sources.addAll(lazySource.sources);
		instances.addAll(lazySource.instances);
	}

	public ProgressData getProgressData() {
		if (instancesDisplay == null) {
			instancesDisplay = StringUtils.join(instances, ",");
		}
		
		return super.getProgressData();
	}
	
	@Override
	public Record next() throws IOException {
		Record rec = curr != null ? curr.next() : null;
		while (rec == null) {
			if (sources.isEmpty()) return null;
			curr = sources.remove(sources.size()-1);
			rec = curr.next();
		}

		reportSourcedRecord(rec);
		return rec;
	}
	
	@Override
	public Object split() {
		if (sources.size() <= 1) return null;
		RecordSource rs = sources.remove(sources.size()-1);
		return new LazyReductionSource(rs);
	}
	
	/**
	 * constructor for split only.
	 * 
	 * @param splitSource
	 */
	private LazyReductionSource(RecordSource splitSource) {
		sources.add(splitSource);
	}
	
	/**
	 * 
	 *
	 */
	public static class InnerReductionSource extends MinimalRecordSource {
    	private final Reducer reducer;
    	
    	public InnerReductionSource(Reducer reducer) {
    		this.reducer = reducer;
    	}
    	
    	@Override
    	public Record next() throws IOException {
    		return reducer.getNextMainReduction();
    	}
    }
}