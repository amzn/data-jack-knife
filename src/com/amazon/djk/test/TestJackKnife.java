package com.amazon.djk.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.processor.DJKInitializationException;
import com.amazon.djk.processor.DataJackKnife;
import com.amazon.djk.processor.JackKnife;

/**
 * Accessing all JackKnife used in testing through this static method
 * ensures that CoreDefs and ThreadDefs are already set from a test
 * will be deinitialized.
 *
 */
public class TestJackKnife {
	private static final Logger LOG = LoggerFactory.getLogger(TestJackKnife.class);
	
	public static DataJackKnife create() throws DJKInitializationException {
		DataJackKnife.deinitialize();
		
		DataJackKnife knife = new DataJackKnife();
		knife.setReportOnce(true);
		
		return knife;
	}
	
	public static JackKnife create(Class<? extends JackKnife> clazz) throws DJKInitializationException {
		DataJackKnife.deinitialize();
		
		try {
			JackKnife knife = clazz.newInstance();
			knife.setReportOnce(true);
			return knife;
        } 
        
        catch (InstantiationException | IllegalAccessException e) {
        	throw new DJKInitializationException("could not instantiate: " + clazz.getSimpleName());
        }
	}
}
