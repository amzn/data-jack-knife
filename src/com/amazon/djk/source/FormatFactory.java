package com.amazon.djk.source;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParseToken;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.SlotTokenizer.ParseSlot;
import com.amazon.djk.expression.SlotTokenizer.SlotType;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.file.FileSystem;
import com.amazon.djk.file.FileSystems;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.format.FormatOperator;
import com.amazon.djk.format.WriterOperator;
import com.amazon.djk.keyed.LazyKeyedSource;
import com.amazon.djk.reducer.PathReducer;
import com.amazon.djk.sink.FormatFileSinkHelper;
import com.amazon.djk.sink.FormatFileSink;

public class FormatFactory {
	private static Logger logger = LoggerFactory.getLogger(FormatFactory.class);
	private final Map<String,FormatOperator> formatOps = new TreeMap<>();
	private final Map<String,WriterOperator> writerOps = new TreeMap<>();
	private final FileSystems fileSystems = new FileSystems();

	public void addFileSystem(FileSystem filesys) {
		fileSystems.addFileSystem(filesys);
	}
	
	/**
	 * 
	 * @param handle
	 */
	
	public void addWriterOperator(WriterOperator op) {
		int size = writerOps.size();
		writerOps.put(op.getName(), op);
		if (writerOps.size() != size + 1) {
			throw new RuntimeException("Improperly configured jackknife." +  
                    "Operator format name collision for '" + op.getName() + 
                    "' from '" + op.getClass().getSimpleName() + "'");
		}
	}
	
	public void addFormatOperator(FormatOperator op) {
		int size = formatOps.size();
		formatOps.put(op.getName(), op);		
		if (formatOps.size() != size + 1) {
			throw new RuntimeException("Improperly configured jackknife." +  
                    "Operator format name collision for '" + op.getName() + 
                    "' from '" + op.getClass().getSimpleName() + "'");
		}
	}

	/**
	 * 
	 * @param token
	 * @param operands
	 * @return
	 * @throws IOException
	 */
	public RecordSink createFormatSink(ParseToken token, ParserOperands operands) throws IOException {
        String formatHint = getFormatHint(token);
        FormatArgs unknownOpFormatArgs = FormatArgs.create(fileSystems, token, formatHint, false);
        
		String format = unknownOpFormatArgs.getFormat();
        WriterOperator wop = writerOps.get(format);
        if (wop == null) {
        	throw new SyntaxError(token, "unknown format");
        }
        
        // get formatArgs using discovered format operator, to get the params
        OpArgs tempArgs = new OpArgs(wop, unknownOpFormatArgs.getToken(), operands);
        Set<String> formatParams = tempArgs.getParamNames();
        for (String param : formatParams) {
            Object value = tempArgs.getParam(param);
            unknownOpFormatArgs.addAnnotationLessParam(param, value);
        }
        
        FormatArgs fargs = unknownOpFormatArgs; // rename because now we know
        final FormatFileSinkHelper finfo = new FormatFileSinkHelper(fargs, wop);
        
        // if NOT asFile, then implement the replicate method
        FormatFileSink sink = finfo.asFile() ?
        		new FormatFileSink(null, finfo) :
        		new FormatFileSink(null, finfo) {
            @Override
            public Object replicate() throws IOException {
            	return new FormatFileSink(this, finfo);
            }
        };
        
        String pathReducerInstance = (String)fargs.getParam(WriterOperator.PATH_REDUCER_INSTANCE_PARAM);
        if (pathReducerInstance != null) {
        	PathReducer reducer = new PathReducer(sink.getAbsolutePath(), pathReducerInstance);
        	reducer.addSource(operands.pop());
        	sink.addSource(reducer);
        }
        
        else {
        	sink.addSource(operands.pop());
        }
    	
        return sink;
	}        
	
	/**
	 * 
	 * @param token
	 * @param operands
	 * @return
	 * @throws IOException
	 * @throws SyntaxError
	 */
    public RecordSource createFormatSource(ParseToken token, ParserOperands operands) throws IOException, SyntaxError {
        String formatHint = getFormatHint(token);
        
        boolean allowMissing = allowsMissing(token);
        
        // at this point the token must be a file based source, we don't know the op
        FormatArgs unknownOpFormatArgs = null;
        try {
            unknownOpFormatArgs = FormatArgs.create(fileSystems, token, formatHint, true);
        } catch (FileNotFoundException | SyntaxError e) {
        	if (allowMissing) {
        		return new EmptyKeyedSource.EmptySource(token.toString());
        	}
        	
        	throw e; // rethrow
        }
        
		SourceProperties props = unknownOpFormatArgs.getSourceProperties();
		String format = props.getSourceFormat();
		
        SourceOperator sourceOp2 = formatOps.get(format);
        if (sourceOp2 == null) {
            throw new RuntimeException("source.properties format '" + format + "' is non-existent");
        }
        
        /* unknownOpAccessArgs are the generic file access args and it's parameters.
         * (i.e. args at the point where we didn't know what the format was) However, the 
         * FormatOperator parameters are yet to be collected from the input string. 
         * Get these and add them to the access args.
         */
        
        // get formatArgs using discovered format operator, to get the params
        OpArgs tempArgs = new OpArgs(sourceOp2, unknownOpFormatArgs.getToken(), operands);
        Set<String> formatParams = tempArgs.getParamNames();
        for (String param : formatParams) {
            Object value = tempArgs.getParam(param);
            unknownOpFormatArgs.addAnnotationLessParam(param, value);
        }
        
        // rename appropriately
        FormatArgs formatArgs = unknownOpFormatArgs;
        return LazyKeyedSource.create(sourceOp2, formatArgs);
	}

    /**
     * this is kind of hack. need to rework the chicken and egg challenges represented in this file
     * 
     * @param token
     * @return
     */
    private boolean allowsMissing(ParseToken token) {
        List<ParseSlot> slots = token.getSlots();
        for (ParseSlot slot : slots) {
        	if (slot.type == SlotType.PARAM) {
        		String[] pieces = slot.string.split("=");
        		if (pieces.length == 2 &&
        			pieces[0].equals(FormatOperator.ALLOW_MISSING) &&
        			pieces[1].equalsIgnoreCase("true")) {
        			return true;
        		}
        	}
        }

        return false;
    }
	
    /**
     * using FormatOpererator.isFormatMatch try to determine a format hint.
     * @param token
     * @return
     * @throws SyntaxError
     */
    private String getFormatHint(ParseToken token) throws SyntaxError {
        int pathSlotNo = token.isScheme() ? 1 : 0;
        ParseSlot pathSlot = token.getSlots().get(pathSlotNo);
        
        int numMatches = 0;
        SourceOperator op = null;
        for (SourceOperator so : formatOps.values()) {
            if (so instanceof FormatOperator &&
                    ((FormatOperator)so).isFormatMatch(pathSlot.string)) {
                numMatches++;
                op = so;
            }
        }
        
        if (numMatches == 1) {
            return op.getName();
        }
        
        return null;
    }
	
	/**
     * 
     * @return
     */
    public Set<String> getFormats() {
    	return formatOps.keySet();
    }
    
    /**
     * 
     * @return
     */
    public Map<String,FormatOperator> getFormatOperators() {
    	return formatOps;
    }
    
    public Map<String,WriterOperator> getWriterOperators() {
    	return writerOps;
    }

	public void close() throws IOException {
		fileSystems.close();
	}

    public Map<String,FileSystem> getFileSystems() {
        return fileSystems.getFileSystems();
    }
}
