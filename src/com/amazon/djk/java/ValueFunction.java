package com.amazon.djk.java;

import java.io.IOException;

import com.amazon.djk.record.Record;

public abstract class ValueFunction {
	
	public abstract Object get(Record rec) throws IOException;
}
