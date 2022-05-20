package com.amazon.djk.report;

import java.util.Date;

import org.apache.commons.lang.time.DurationFormatUtils;

public class ElapsedTime {
	private long startMillis;
	private long stopMillis;
	private boolean isStopped = false; 
	
	public ElapsedTime() {
		startMillis = -1;
	}
	
	public ElapsedTime start() {
		startMillis = new Date().getTime();
		return this;
	}
	
	public void stop() {
		stopMillis = new Date().getTime();
		isStopped = true;
	}
	
	@Override
	public String toString() {
		if (startMillis == -1) {
			return "00:00:00"; // hasn't started
		}
		
		long elapsedMillis = isStopped ?
				stopMillis - startMillis :
				(new Date().getTime() - startMillis);
		return DurationFormatUtils.formatDuration(elapsedMillis, "HH:mm:ss", true);
	}
}
