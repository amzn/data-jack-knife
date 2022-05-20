package com.amazon.djk.processor;

import java.io.IOException;

/**
 * This exception is thrown when the DJK Framework has not been initialized correctly
 * for a thread.
 * 
 * Only threads that either instantiate a DataJackKnife or call DataJackKnife.initialize()
 * may access CoreDefs.get(), to access the core definitions.
 * 
 * Auxillary threads should be implemented by extending the DJKRunnable class.
 * Doing so passes the main thread CoreDefs to the worker threads and allows
 * ThreadDefs.get() to be called.
 * 
 * If these rules are broken a DJKInitializationException is thrown.
 *
 */
public class DJKInitializationException extends IOException {
	private static final long serialVersionUID = 1L;
	private static String messageFormat = "This thread has attempted to access an uninitialized '%s'.\n" +
			"To rememdy this do one of the following:\n" +
			"1) In a main thread, instantiate one and only one JackKnife prior to accessing %s\n" +
			"2) For a worker thread within a DJK component, extend DJKRunnable and instantiate the runnable.\n" +
			"   in the same thread as the JackKnife. Or\n" +
			"3) Use ThreadDefs.initialize(CoreDefs); to initialize the ThreadDefs with the CoreDefs from the main thread.";
	private static String multipleMessage = "Only one JackKnife is allowed to be instantiated per thread.\n"+
			" A new JackKnife may be instantiated within the same thread after calling JackKnife.deinitialize();";

	public enum Type { CORE_DEFS("CoreDefs"), 
					   THREAD_DEFS("ThreadDefs"),
					   MULTIPLE_KNIVES("multipleKnives");
		private final String message;

		Type(String message) {
			this.message = message;
		}
	};

	private static String getMessage(Type type) {
		return type == Type.MULTIPLE_KNIVES ?
				multipleMessage : String.format(messageFormat, type.message, type.message); 		
	}
	
	public DJKInitializationException(Type type) {
		super(getMessage(type));
	}
	
	public DJKInitializationException(String message) {
		super(message);
	}
}
