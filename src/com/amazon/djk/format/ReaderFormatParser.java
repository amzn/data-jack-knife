package com.amazon.djk.format;

import java.io.IOException;

import com.amazon.djk.processor.CoreDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;
import com.amazon.djk.record.ThreadDefs;
import org.apache.commons.lang.math.NumberUtils;

public abstract class ReaderFormatParser extends FormatParser {
	private final static Logger LOGGER = LoggerFactory.getLogger(ReaderFormatParser.class);
	private long numFormatExceptions = 0;
    public static final int BUFFER_SIZE = 512 * 1024;
    
	/**
	 *
     * @param fifo the outgoing records
	 * @param reader the incoming data
	 * @return true if at least one record has been filled
	 * @throws IOException
	 */
	public boolean fill(PushbackLineReader reader, RecordFIFO fifo, int numAllowErrors) throws IOException {
        if (numAllowErrors != 0 && numFormatExceptions == numAllowErrors) return false;
        fifo.reset();
		Record rec = null;

        while (true) {
        	try {
        		rec = next(reader);
        	} catch (FormatException e) {
                LOGGER.info(e.getMessage());
                numFormatExceptions++;

                if (numFormatExceptions < numAllowErrors) {
                    continue;
				}

                // allow this error, but we're done in order to guarentee we won't see another
                else if (numFormatExceptions == numAllowErrors) {
                    return fifo.byteSize() != 0;
                }

                else if (numFormatExceptions > numAllowErrors) {
                    throw new IOException(e);
                }

        		continue;
        	}
        	
        	if (rec == null) {
        		return fifo.byteSize() != 0;
        	}
        	
        	fifo.add(rec);
        	if (fifo.byteSize() >= BUFFER_SIZE) {
        		return fifo.byteSize() != 0;
        	}
        }
    }

	public long getNumFormatExceptions() {
		return numFormatExceptions;
	}
	
	/**
	 * 
	 * @param reader the source of records
	 * @return the next record.  Returns null if reader is null
	 * @throws IOException
	 * @throws FormatException 
	 */
	public abstract Record next(PushbackLineReader reader) throws IOException, FormatException;
	
	/**
	 * Parsers that need to initialize once per reader can do so by overriding this method
	 * 
	 * @param reader
	 * @throws IOException
	 */
	public void initialize(PushbackLineReader reader) throws IOException { }

    /**
     * called by DJK
     *
     * @param reader
     * @throws IOException
     */
	public void doInitialize(PushbackLineReader reader) throws IOException {
        this.initialize(reader);
        numFormatExceptions = 0;
    }

    /**
     * adds valueToBeTyped into field, where valueToBeTyped is interpretted as either double, long, boolean or string
     *
     * @param out
     * @param name
     * @param valueToBeTyped
     * @throws IOException
     */
    public static void addPrimitiveValue(Record out, String name, String valueToBeTyped) throws IOException {
        Class<?> clazz = ThreadDefs.get().getFieldType(name);

        if (clazz != null) {
            if (clazz == String.class) {
                out.addField(name, valueToBeTyped);
            }

            else if (clazz == Long.class) {
                out.addField(name, Long.parseLong(valueToBeTyped));
            }

            else if (clazz == Double.class) {
                out.addField(name, Double.parseDouble(valueToBeTyped));
            }

            return;
        }

        Object val = getMostSpecificPrimitive(valueToBeTyped);
        if (val instanceof String) {
            out.addField(name, (String)val);
        }

        else if (val instanceof Double) {
            out.addField(name, (Double)val);
        }

        else if (val instanceof Long) {
            out.addField(name, (Long)val);
        }

        else if (val instanceof Boolean) {
            out.addField(name, (Boolean)val);
        }
    }
	
	/**
     * @param value
     */
    public static Object getMostSpecificPrimitive(String value) {

        try {
            if (NumberUtils.isNumber(value)) {
                if (value.contains(".") || value.contains("e") || value.contains("E")) {
                    Double d = Double.valueOf(value);
                    if (d.isInfinite()) {
                        return value;
                    }
                    return d;
                } else {
                    return Long.valueOf(value);
                }
            }
        } catch (NumberFormatException e) {
            //Exception happen when number is legal Java number but not parse-able.
        }
        return getBooleanOrString(value);
    }

    private static Object getBooleanOrString(String value) {
        if (value.equalsIgnoreCase("true")) {
            return true;
        }

        else if (value.equalsIgnoreCase("false")) {
            return false;
        }
        else {
            return value; // string
        }
    }
}
