package com.amazon.djk.source;

import com.amazon.djk.core.BaseRecordSource;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.core.Splittable;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.NodeReport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * source list of sources
 *
 */
public class SourceListSource extends BaseRecordSource implements Splittable {
	private final List<RecordSource> sources = new ArrayList<>();
	private RecordSource curr = null;

	/**
	 * 
	 * @return the list of sources.  Changes to this list will be reflected
	 * in the operation of this class.
	 */
	public List<RecordSource> getAsList() {
		return sources;
	}
	
	@Override
	public Object split() {
		if (sources.size() <= 1) return null;
		return sources.remove(sources.size()-1);
	}
	
	@Override
	public NodeReport getReport() {
		if (report != null) return report;
		report = super.getReport();
		
		for (RecordSource source : sources) {
			report.addChildReport( source.getReport() );
		}

		return report;
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
	
	public void addSource(RecordSource source) {
	    // flatten
	    if (source instanceof SourceListSource) {
	        List<RecordSource> sl = ((SourceListSource)source).getAsList();
	        for (RecordSource s : sl) {
	            addSource(s); // recurse
	        }
	        
	        return; // bottom of recursion!
	    }
	    
		sources.add(source);
	}
	
	/**
	 * 
	 * @return the number of sources
	 */
	public int size() {
		return sources.size();
	}
	
	@Override
	public void close() throws IOException {
		for (RecordSource source : sources) {
			source.close();
		}
	}
}
