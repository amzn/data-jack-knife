package com.amazon.djk.legacy;

import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.Param;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.format.FormatException;
import com.amazon.djk.format.FormatOperator;
import com.amazon.djk.format.FormatParser;
import com.amazon.djk.format.PushbackLineReader;
import com.amazon.djk.format.ReaderFormatParser;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * NOT THREADSAFE 
 */
public class NVPFormatParser extends ReaderFormatParser {
    public final static String NVP_SUB_RECS_PARAM = "validSubRecNames";
    public final static String NVP_SUB_FIELDS_PARAM = "validSubRecFields";
    public final static String DEFAULT_SUB_NAMES = "clause,keyword";
    public final static String DEFAULT_SUB_FIELDS = "targetingClauseId,subId,bid,expression,type,override,clause";
    public static final String END_OF_RECORD = "---:";
    public static final String FORMAT = "nvp";
    public static final String STREAM_FILE_REGEX = "nvp.*";
    private static Logger logger = LoggerFactory.getLogger(NVPFormatParser.class);
    private final Set<String> okSubNames;
    private final Set<String> okSubFields;
    private final SourceProperties props;
    
    private final Record rec = new Record();
    private final List<String> recLines = new ArrayList<>();
	private final Record subrec = new Record();

	/**
	 * constructor provides default parsing of janus files
	 * @throws IOException
	 */
    public NVPFormatParser() throws IOException {
        this.props = null;
        okSubNames = new HashSet<>(Arrays.asList(DEFAULT_SUB_NAMES.split(",")));
        okSubFields = new HashSet<>(Arrays.asList(DEFAULT_SUB_FIELDS.split(",")));
    }
    
    public NVPFormatParser(String[] okSubNames, String[] okSubFields) throws IOException {
        props = null;
        this.okSubNames = new HashSet<>(Arrays.asList(okSubNames));
        this.okSubFields = new HashSet<>(Arrays.asList(okSubFields));
    }
	
    public NVPFormatParser(SourceProperties props) throws IOException {
        this(props, true);
    }
	
    public NVPFormatParser(SourceProperties props, boolean requireFieldWhitelists) throws IOException {
        this.props = props;
        
        // hack because NVP sub-record format is inherently unparsable
        okSubNames = requireFieldWhitelists ? getOkFields(NVP_SUB_RECS_PARAM, props) : null;
        okSubFields = requireFieldWhitelists ? getOkFields(NVP_SUB_FIELDS_PARAM, props) : null;
    }
    
    /**
     * get the ok subrecords and ok subrecord fields from either the source properties or the params
     * 
     * @param name
     * @param props
     * @return
     * @throws IOException
     */
    private Set<String> getOkFields(String name, SourceProperties props) throws IOException {
        String propsFileEntry = props.getExtra(name);
        String[] fieldNames = null;
        
        if (propsFileEntry != null) {
            fieldNames = propsFileEntry.split(",");
        }
        
        else {
            FormatArgs fileAccessArgs = props.getAccessArgs();
            // defaults so we can read janus data without the need to set these.
            fieldNames = (String[])fileAccessArgs.getParam(name);
        }

        if (fieldNames == null) return null;
        
        Set<String> okNames = new HashSet<>();
        for (String s : fieldNames) {
            okNames.add(s);
        }
        
        return okNames; 
    }
    
    @Override
	public Record next(PushbackLineReader reader) throws IOException, FormatException {
		if (reader == null) return null;

		// holds all lines of record without :---
		recLines.clear();
		
        while (true) {
        	String line = reader.readLine();
        	if (line == null) {
    			// allow missing END_OF_RECORD, if valid
        		if (recLines.size() != 0) {
                	return parse(recLines);
        		}

        		return null; // we're done
        	}
        
        	 // END OF RECORD
            if (line.startsWith(END_OF_RECORD)) {
            	return parse(recLines);
            }
            	
        	recLines.add(line);
        }
    }
    
	/**
	 * 
	 * @param recLines
	 * @return
	 * @throws IOException
	 * @throws FormatException
	 */
	private Record parse(List<String> recLines) throws IOException, FormatException {
		rec.reset();
		
		for (String line : recLines) {
        	line = line.trim();
        	
            // min length line N:V = 3 bytes (<lf> does not count towards the length)
            if (line.length() < 3) continue;
            
            int colon = line.indexOf(':');
            if (colon == -1) {
                throw new FormatException("missing colon");
            }
            
            String name = line.substring(0, colon);
            String value = line.substring(colon+1);

            if (!isValidFieldName(name)) {
                throw new FormatException("invalid field name");
            }

            // at this point, name is valid

            if (!isSubRecord(name, value)) {
                ReaderFormatParser.addPrimitiveValue(rec, name, value);                
            }
            
            else {
                SubStatus status = addAsSubRecord(name, value);
                switch (status) {
                case IS_SUB:
                    // was added as sub
                    break;
                
                case IS_FIELD:
                    ReaderFormatParser.addPrimitiveValue(rec, name, value);
                    break;
                
                case IS_ERROR:
                default:
                	throw new FormatException("sub-record error");
                }
            }
        }
		
		return rec;
	}
	
	private boolean isSubRecord(String name, String value) {
        if (okSubNames != null) {
            return okSubNames.contains(name);
        }
        
        // approximate. pipe symbol test
        return value.indexOf('|') != -1;
	}
	
	enum SubStatus {IS_SUB, IS_FIELD, IS_ERROR}; 

	/**
	 * this is approximate.  try to postpone splits.  
	 * value must = name<colon> ... [<pipe>name:...]
	 * 
	 * @param name
	 * @param value
	 * @return
	 * @throws IOException
	 */
	private SubStatus addAsSubRecord(String name, String value) throws IOException {
        int pipe = value.indexOf('|');
        if (pipe == -1) { // regular field
            return SubStatus.IS_FIELD;
        }
        
    	String[] childs = value.split("\\|");
        subrec.reset();
    	for (String child : childs) {
    		String[] pair = child.split(":");
    		if (pair.length != 2) {
    			return SubStatus.IS_FIELD;
    		}
    		
    		String n = pair[0];
    		String v = pair[1];
    		
    		if (okSubFields != null && !okSubFields.contains(n)) {
    		    logger.info("BAD SUB FIELD=" + n);
                return SubStatus.IS_ERROR;
    		}
    		
    		if (!isValidFieldName(n)) {
                return SubStatus.IS_ERROR;
    		}
    		
    		else {
    		    ReaderFormatParser.addPrimitiveValue(subrec, n, v);
    		}
    	}
    	
        rec.addField(name, subrec);
		return SubStatus.IS_SUB;
	}

    /**
     * Legal Field characters are: letters, numbers, '.', '_'
     * but the first character cannot be a number.
     * 
     * @param name
     * @return
     */
    private boolean isValidFieldName(String name) {
        if (Character.isDigit(name.charAt(0))) {
            return false;
        }

        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c != '.' && c != '_' &&
                !Character.isAlphabetic(c) &&
                !Character.isDigit(c)) {
                return false;
            }
        }
        
        return true;
    }
    
	@Override
	public Object replicate() throws IOException {
		return new NVPFormatParser(props);
	}

	@Description(text={"reads nvp file(s) as a source of records."})
	@Param(name=NVP_SUB_RECS_PARAM, gloss = "comma separated list of valid nvp subrecord names", type=ArgType.STRINGS, defaultValue = DEFAULT_SUB_NAMES)
	@Param(name=NVP_SUB_FIELDS_PARAM, gloss = "comma separted list of valid subrecord fields", type=ArgType.STRINGS, defaultValue = DEFAULT_SUB_FIELDS)
	public static class Op extends FormatOperator {
		public Op() {
			super(FORMAT, STREAM_FILE_REGEX);
		}

		@Override
		public FormatParser getParser(SourceProperties props) throws IOException {
			return new NVPFormatParser(props);
		}
	}
}
