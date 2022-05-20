package com.amazon.djk.natdb;

import com.amazon.djk.record.Record;

public class MapRecord extends Record {
	private boolean wasAccessed = false;
	
	/**
	 * set that this record was accessed (think right join)
	 */
	public void setWasAccessed() {
		wasAccessed = true;
	}
	
	/**
	 * 
	 * @return true if setWasAccessed() has been called
	 */
	public boolean getWasAccessed() {
		return wasAccessed;
	}
}
