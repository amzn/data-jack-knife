package com.amazon.djk.source;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.amazon.djk.core.RecordSource;
import com.amazon.djk.file.FileQueue;
import com.amazon.djk.file.FileQueue.LazyFile;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.format.FileFormatParser;
import com.amazon.djk.format.FormatOperator;
import com.amazon.djk.format.FormatParser;
import com.amazon.djk.format.PushbackLineReader;
import com.amazon.djk.format.ReaderFormatParser;
import com.amazon.djk.record.RecordFIFO;

public class FileRecordProducer extends RecordProducer {
	private final FileQueue files;
	private final FormatParser parser;
	private volatile long numFormatExceptions = 0;
	private final String versionId;
	private final LazyFileProducer producer;
	private final FormatArgs accessArgs;
	private final int numAllowErrors;

	private interface LazyFileProducer {
		void initialize(LazyFile file) throws IOException, InterruptedException;
		
		/**
		 * 
		 * @return the next queueable source.  Implementers return null before init is called.
		 * @throws Exception
		 */
		RecordSource getNextQueueableSource() throws Exception;
	}
	
	/**
	 * 
	 *
	 */
	private class FileFormatProducer implements LazyFileProducer {
		private final FileFormatParser parzer;
		private boolean inited = false;

		public FileFormatProducer(FileFormatParser parser) throws IOException {
			this.parzer = parser;
		}
		
		@Override
		public void initialize(LazyFile file) throws IOException, InterruptedException {
			parzer.doInitialize(file);
			inited = true;
		}

		@Override
		public RecordSource getNextQueueableSource() throws Exception {
			if (!inited) return null;
			
			RecordFIFO buffer = new RecordFIFO();
			if (parzer.outerFill(buffer, numAllowErrors)) {
				numFormatExceptions += parzer.getNumFormatExceptions();
				return buffer;
			}
			
			inited = false;
			return null;
		}
	}
	
	/**
	 * 
	 *
	 */
	private class ReaderFormatProducer implements LazyFileProducer {
        private final ReaderFormatParser parzer;
		private PushbackLineReader reader = null;
		
		public ReaderFormatProducer(ReaderFormatParser parser) {
			this.parzer = parser;
		}
		
		@Override
		public void initialize(LazyFile file) throws IOException, InterruptedException {
			InputStream stream = file.getStream();
			InputStreamReader isr = new InputStreamReader(stream);
			reader = new PushbackLineReader(isr);
	        ReaderFormatParser parzer = (ReaderFormatParser)parser;
	        parzer.doInitialize(reader);
		}
		
		@Override
		public RecordSource getNextQueueableSource() throws Exception {
			if (reader == null) return null;
			
			RecordFIFO buffer = new RecordFIFO();
			if (parzer.fill(reader, buffer, numAllowErrors)) {
				numFormatExceptions += parzer.getNumFormatExceptions();
				return buffer;
			}

			reader.close();
			reader = null;
			return null;
		}
	}
	
	/**
	 * first constructor
	 * 
	 * @param queueSize
	 * @param files
	 * @param parser
	 * @throws IOException
	 */
	public FileRecordProducer(int queueSize, FileQueue files, FormatParser parser, FormatArgs accessArgs) throws IOException {
		super(queueSize);
		this.files = files;
		this.parser = parser;
		this.versionId = files.getVersionId();
		this.accessArgs = accessArgs;
		numAllowErrors = (int)accessArgs.getParam(FormatOperator.ALLOW_ERRORS);
		
		producer = (parser instanceof ReaderFormatParser) ?
				new ReaderFormatProducer((ReaderFormatParser)parser) :
				new FileFormatProducer((FileFormatParser)parser);
	}
	
	/**
	 * subsequent constructor
	 * 
	 * @param first
	 * @param files
	 * @param parser
	 * @throws IOException
	 */
	public FileRecordProducer(RecordProducer first, FileQueue files, FormatParser parser, FormatArgs accessArgs) throws IOException {
		super(first);
		this.files = files;
		this.parser = parser;
		this.versionId = files.getVersionId();
		this.accessArgs = accessArgs;
		numAllowErrors = (int)accessArgs.getParam(FormatOperator.ALLOW_ERRORS);

		producer = (parser instanceof ReaderFormatParser) ?
				new ReaderFormatProducer((ReaderFormatParser)parser) :
				new FileFormatProducer((FileFormatParser)parser);
	}

	@Override
	public RecordSource getNextQueueableSource() throws Exception {
		RecordSource source = producer.getNextQueueableSource();
		
		while (source == null) {
			LazyFile file = files.next();
			if (file == null) return null;
			
			producer.initialize(file);
			source = producer.getNextQueueableSource();
		}
		
		return source;
	}

	@Override
	public void initialize() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	public String getVersionId() {
		return versionId;
	}

	public long getNumFormatExceptions() {
		return numFormatExceptions;
	}
}
