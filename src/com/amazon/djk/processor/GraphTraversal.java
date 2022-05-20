package com.amazon.djk.processor;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;

/**
 * could move all traversals here for clarity
 *
 */
public class GraphTraversal {
	
	/**
	 * traverses from the main sink to originating record source
	 * @param sink
	 * @return
	 */
	public static RecordSource getMainRecordSource(RecordSink sink) {
		RecordSource source = sink;
		
		while (true) {
			if (source instanceof RecordPipe) {
				source = ((RecordPipe) source).getSource();
				continue;
			}

			if (source instanceof WithInnerSink) {
				RecordSink innerSink = ((WithInnerSink)source).getSink();
				source = innerSink.getSource();
				continue;
			}
			
			return source;
		}		
	}
}
