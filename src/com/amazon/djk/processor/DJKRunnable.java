package com.amazon.djk.processor;

import java.io.IOException;

import com.amazon.djk.record.ThreadDefs;

/**
 * This class helps with the complexity of the CoreDefs, ThreadDefs within DJK
 * with respect to starting new threads.  This abstract base class is instantiated
 * within the main DJK thread and is passed its associated CoreDefs.  Before any
 * DJK processing occurs within the new thread, the ThreadDefs is set.  Implementing
 * subclasses define their work within the innerRun method.
 *
 */
public abstract class DJKRunnable implements Runnable {
	private final CoreDefs cdefs;
	
	/**
	 * Construction must occur in the main thread of the JackKnife.
	 * 
	 * @param cdefs
	 * @throws IOException 
	 */
	public DJKRunnable() throws IOException {
		this.cdefs = CoreDefs.get();
	}
	
	@Override
	final public void run() {
		ThreadDefs.initialize(cdefs);
		innerRun();
	}

	abstract public void innerRun();
}
