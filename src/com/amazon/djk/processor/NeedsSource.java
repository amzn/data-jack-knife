package com.amazon.djk.processor;

import com.amazon.djk.core.RecordSource;

/**
 * Interface for Sources (e.g. WithInnerSink) that themselves consume a Source  
 */
public interface NeedsSource {
    void addSource(RecordSource source);
}
