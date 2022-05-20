package com.amazon.djk.core;

import java.io.IOException;

/**
 * 
 * RecordSources that implement Splittable return a subset 
 * of their records via the split off RecordSource
 */
public interface Splittable {
	Object split() throws IOException;
}
