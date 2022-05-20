package com.amazon.djk.report;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.amazon.djk.core.BaseRecordSource;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.processor.ExecutionContext;
import com.amazon.djk.record.Record;

/**
 * A node in a recursive representation of progress
 */
public class NodeReport {
	protected final ReportProvider rootProvider;
	protected final String nodeLabel; // defaults to class name
	protected final List<NodeReport> childReports = new CopyOnWriteArrayList<>();
	protected NodeReport leftReport = null;
	protected final List<ReportProvider> providers = new CopyOnWriteArrayList<>();
	private ProgressData data;
	protected final boolean isSuppressed;
	private NodeReport parentNode;

	/**
	 * 
	 * @param rootProvider
	 */
	public NodeReport(ReportProvider rootProvider) {
		this(rootProvider, getDefaultLabel(rootProvider));
	}
	
	/**
	 * 
	 * @param nodeLabel of the processor. Generally the class name
	 * @param rootProvider
	 */
	public NodeReport(ReportProvider rootProvider, String nodeLabel) {
		this.rootProvider = rootProvider;
		this.nodeLabel = nodeLabel != null ? nodeLabel : getDefaultLabel(rootProvider);
		isSuppressed = rootProvider.isReportSuppressed();
		providers.add(rootProvider);
	}
	
	public NodeReport getParentReport() {
		return parentNode;
	}
	
    @Override
    public String toString() {
        return nodeLabel; // display the node name
    }
    
	protected double getRecsPerSecond() {
		if (rootProvider instanceof RecordSink) {
			return ((RecordSink)rootProvider).sinkRecsPerSecond();
		}
		
		/**
		 * else not sure. Could be a SourceListSource which is not a pipe.
		 */
		return 0.0;
	}
	
	public void addLeftReport(NodeReport report) {
		leftReport = report;
	}
	
	/**
	 * 
	 * @param report
	 */
	public void addChildReport(NodeReport report) {
	    if (report != null) { 
	    	// add(0) changes the order children are displayed
	    	report.parentNode = this;
	        childReports.add(0, report); 
	    }
	}

	/**
	 * get the report which is displayed with a link from the left
	 * 
	 */
	public NodeReport getLeftReport() {
		return leftReport;
	}
	
	/**
	 * 
	 * @return true if the node has no display along its left graph
	 */
	public boolean isLeftReportSuppressed() {
		if (leftReport == null) return true;
		return leftReport.isSuppressed && 
			   leftReport.isLeftReportSuppressed();
	}
	
	/**
	 * 
	 * @param provider
	 * @return a default name based on the provider class
	 */
	public static String getDefaultLabel(ReportProvider provider) {
		String name = provider.getClass().getSimpleName();
		if (name == null || name.length() == 0) {
			name = provider.getClass().getSuperclass().getSimpleName();
		}
		
		// cut down the verbosity
		String[] endings = {"Pipe", "Sink", "Source", "Reducer"};
		for (String ending : endings) {
	        if (name.endsWith(ending)) {
	            return name.substring(0, name.length()-ending.length());	            
	        }
		}
		
		return name;
	}
		
	/**
	 * 
	 * @param provider providing ProgressData;
	 */
	public void addProvider(ReportProvider provider) {
		providers.add(provider);
	}
	
	public ProgressData getProgressData() {
		update();
		return data;
	}
	
	/**
	 * recursively instructs the report to collect progress data from its providers
	 */
	public final void update() {
		// the root provider
		data = providers.get(0).getProgressData();

	    for (int i = 1; i < providers.size(); i++) {
		    ReportProvider provider = providers.get(i);
		    ProgressData contribution = provider.getProgressData();
		    data.aggregate(contribution);
		}
			
		// descend the children
		for (NodeReport child : childReports) {
			child.update();
		}
	}
	
	/**
	 * 
	 * @return a record representation of this node and its children as sub-nodes
	 * 
	 * @throws IOException
	 */
    protected void addNodeTo(Record out) throws IOException {
    	Record node = new Record();
	    node.addField("type", getType());

	    if (data != null) { // append the report data
	    	node.addFields(data.getAsRecord());
	    }

	    if (rootProvider instanceof RecordSource) {
	    	node.addField("thread_Mrecs_perSec", getRecsPerSecond() / 1000000.0);
	    }
	    
        for (int i = 0; i < childReports.size(); i++) { 
            NodeReport child = childReports.get(i);
            Record childRec = new Record();
            child.addNodeTo(childRec);
            node.addFields(childRec);
        }
        
        if (rootProvider instanceof BaseRecordSource) {
        	Record origin = ((BaseRecordSource)rootProvider).getOriginReport();
        	if (origin != null) {
        		// makes for recursive structure via "node" field
        		node.addFields(origin);
        	}
        }

        out.addField("node", node);
        
        NodeReport left = getLeftReport();
        if (left != null) {
        	Record leftRec = new Record();
	        left.addNodeTo(leftRec);
	        out.addFields(leftRec);
	    }        
	}

    /**
     * 
     * @return
     */
	private String getType() {
    	return  rootProvider instanceof ExecutionContext ? "context" :
    		    rootProvider instanceof RecordSink ? "sink" :    		
				rootProvider instanceof RecordPipe ? "pipe" : "source";
	}
}
