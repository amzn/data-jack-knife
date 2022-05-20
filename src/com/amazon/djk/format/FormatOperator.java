package com.amazon.djk.format;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.file.FileOperator;
import com.amazon.djk.file.FileQueue;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.record.ThreadDefs;
import com.amazon.djk.source.EmptyKeyedSource.EmptySource;
import com.amazon.djk.source.FileConsumerSource;
import com.amazon.djk.source.FileRecordProducer;
import com.amazon.djk.source.FormatParserSource;
import com.amazon.djk.source.RecordProducer;

/**
 * This class is the orchestrator of independent file based read threads. 
 *
 */
@Arg(name="PATH", gloss="the location of the data store.", type=ArgType.STRING)
@Param(name=FileOperator.FILE_VALID_REGEX_PARAM, gloss = "regex of valid record files. This parameter is only required if the input is a directory. Can be placed in source.properties file.", type=ArgType.STRING)
@Param(name=FormatOperator.ALLOW_MISSING, gloss = "if true, a non-existent PATH is allowed yielding an empty source." , type=ArgType.BOOLEAN, defaultValue="false")
@Param(name=FormatOperator.ALLOW_ERRORS, gloss = "number of allowable errors per file. File processing stops at N errors." , type=ArgType.INTEGER, defaultValue="0")
public abstract class FormatOperator extends FileOperator {
    private final int DEFAULT_QUEUE_DEPTH = 200; // way more than threads
    public final static String ALLOW_MISSING = "allowMissing";
    public final static String ALLOW_ERRORS = "allowErrors";

    /**
     * 
     * @param format
     * @param fileRegex a regex pattern that valid files of format adhere to.  This
     * should be not be a promiscuous pattern.  This pattern is used to do two things:
     * 1) filter the InputStreamQueue holding the files to parsed, 2) when deciding
     * which format a directory of files belongs to.
     */
    public FormatOperator(String format, String fileRegex) {
		super(format, fileRegex);
	}
	@Override
	public RecordSource getSource(OpArgs args) throws IOException, SyntaxError {
        FormatArgs accessArgs = (FormatArgs)args;

        if (ThreadDefs.get().getNumSourceThreads() > 0) {
            return getConsumerSource(accessArgs);
        }
                
        else {
            return getNonConsumingSource(accessArgs);
        }
	}

	/**
	 * 
	 * @param accessArgs
	 * @return
	 * @throws IOException
	 * @throws SyntaxError 
	 * @throws InterruptedException 
	 */
	private RecordSource getConsumerSource(FormatArgs accessArgs) throws IOException, SyntaxError {
		FileQueue fileQueue = accessArgs.getInputStreamQueue(defaultFormatRegex);
        if (fileQueue.initialSize() == 0) {
        	return new EmptySource(accessArgs.toString());
        }

        int numAvailableReadThreads = ThreadDefs.get().getNumSourceThreads();
        int numReadThreads = Math.min(numAvailableReadThreads, fileQueue.initialSize());

        List<RecordProducer> producers = new ArrayList<>();
        SourceProperties props = accessArgs.getSourceProperties();
        FormatParser parser = getParser(props);
        FileRecordProducer first = new FileRecordProducer(DEFAULT_QUEUE_DEPTH, fileQueue, parser, accessArgs);
        producers.add(first);

        for (int i = 1; i < numReadThreads; i++) {
        	parser = getParser(props);
        	RecordProducer subsequent = new FileRecordProducer(first, fileQueue, parser, accessArgs);
        	producers.add(subsequent);
        }

        String format = props.getSourceFormat();
        String uri = String.format("%s?format=%s", props.getSourceURI(), format); 
        FileConsumerSource source = new FileConsumerSource(uri, producers, props);

        return source;
	}

	/**
	 *
	 * @param accessArgs
	 * @return
	 * @throws IOException
	 * @throws SyntaxError
	 */
	private RecordSource getNonConsumingSource(FormatArgs accessArgs) throws IOException, SyntaxError {
		FileQueue streams = accessArgs.getInputStreamQueue(defaultFormatRegex);

		if (streams.initialSize() == 0) {
			throw new FileNotFoundException(accessArgs.getURI());
		}
		
        SourceProperties props = accessArgs.getSourceProperties();
        FormatParser parser = getParser(props);
    	return new FormatParserSource(parser, streams, props);
	}
	
	/**
	 * 
	 * @param props
	 * @return the enqueueable reader
	 * @throws IOException 
	 * @throws SyntaxError 
	 */
	public abstract FormatParser getParser(SourceProperties props) throws IOException, SyntaxError;
}
