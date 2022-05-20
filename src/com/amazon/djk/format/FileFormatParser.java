package com.amazon.djk.format;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.file.FileQueue.LazyFile;
import com.amazon.djk.record.RecordFIFO;

public abstract class FileFormatParser extends FormatParser {
	private final static Logger LOGGER = LoggerFactory.getLogger(FileFormatParser.class);
	private long numFormatExceptions = 0;

	/**
	 *
	 * @param fifo
	 * @return true if at least one record has been filled, must return false if parser has not been initialized
	 * @throws IOException
	 * @throws FormatException
	 */
	public abstract boolean fill(RecordFIFO fifo) throws IOException, FormatException;
	
	/**
     * called the first time a parser is presented with a stream, for extensions
	 *
	 * @throws IOException 
     */
    public abstract void initialize(LazyFile file) throws IOException;

	/**
	 * this method called by the DJK
	 *
	 * @param file
	 * @throws IOException
	 */
	public void doInitialize(LazyFile file) throws IOException {
    	this.initialize(file);
    	numFormatExceptions = 0;
	}

	public long getNumFormatExceptions() {
		return numFormatExceptions;
	}
	
	public boolean outerFill(RecordFIFO fifo, int numAllowErrors) throws IOException {
		if (numAllowErrors != 0 && numFormatExceptions == numAllowErrors) return false;

		while (true) {
			try {
				return fill(fifo);
			}

			catch (FormatException e) {
				LOGGER.info(e.getMessage());
				numFormatExceptions++;

				if (numFormatExceptions < numAllowErrors) {
					continue;
				}

				else if (numFormatExceptions == numAllowErrors) {
					return fifo.byteSize() != 0;
				}

				else if (numFormatExceptions > numAllowErrors) {
					throw new IOException("Format Error.\nConsider using 'allowErrors' parameter.\nunix> djk mysource?allowErrors=1", e);
				}
			}
		}
	}
}
