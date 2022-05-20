package com.amazon.djk.keyed;

import java.io.IOException;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;

public abstract class OuterKeyedSource extends KeyedSource {

	public OuterKeyedSource(OpArgs args, Fields keyFields) throws IOException {
		super(args, keyFields);
	}
	
	public OuterKeyedSource(OpArgs args, String[] keyFieldNames) throws IOException {
		super(args, keyFieldNames);
	}

	/**
	 * to be called before any data access occurs to enable the access of
	 * outer/right records.
	 */
	public abstract void enableOuterAccess();
	
	/**
	 * to be called after all joining access occurs to prepare the access
	 * of outer/right records
	 */
	public abstract void prepareOuterAccess();

	/**
	 * A lookup method that will return the value associated with a key one and only one time during the life
	 * of the source.
	 * @param keyRecord the key to be looked up
	 * @return the value record associated with the key or null if non-existent or if looked up previously
	 * @throws IOException
	 */
	public abstract Record getValueOnce(Record keyRecord) throws IOException;
}
