package com.amazon.djk.reducer;

import java.io.IOException;

import com.amazon.djk.core.RecordPipe;

/**
 * For purposes of readabilty. 
 *
 */
public class ReducerAggregator extends RecordPipe {

	public ReducerAggregator(RecordPipe root) throws IOException {
		super(root);
	}
}
