package com.amazon.djk.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.Param;

import com.amazon.djk.file.FileQueue;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;


/**
 * Parse Json strings to records
 *
 * Within a record, the mapping looks like this:
 *
 * <ul>
 *     <li>key:JsonPrimitive -> fieldName:key value:primitive value</li>
 *     <li>key:JsonArray -> fieldName:key value:value1 in array, fieldName value:value2 in array</li>
 *     <li>key:JsonObject -> fieldName:key value:subrecord</li>
 * </ul>
 *
 */
public class JsonFormatParser extends FileFormatParser {

    private final static String JSON_LINES_PARAM = "jsonLines";
    public final static String STREAM_FILE_REGEX = "\\.json(\\.gz)?$";
    public static final String FORMAT = "json";
    public static final int BUFFER_SIZE = 512 * 1024;
    private final Gson gson;
    private JsonReader jsonReader;

    /**
     * Used by replicate and getParser method
     * @throws IOException
     */
    JsonFormatParser() throws IOException {
        this(null);
    }

    /**
     * Used by RestRequestResponse in DJKRestService in order to construct a parser with a predefined JsonReader
     * @param jsonReader
     * @throws IOException
     */
    public JsonFormatParser(JsonReader jsonReader) throws IOException {
        this.jsonReader = jsonReader;
        if(jsonReader != null) {
            this.jsonReader.beginArray();
        }
        gson = new GsonBuilder()
                .registerTypeAdapter(Record.class, new JsonDeserializer())
                .create();
    }

    @VisibleForTesting
    void setJsonReader(JsonReader jsonReader) {
        this.jsonReader = jsonReader;
    }

    public Record next() throws IOException, FormatException {
        try{
            if(jsonReader == null) {
                return null;
            }
            return jsonReader.hasNext() ? gson.fromJson(jsonReader, Record.class) : null;
        } catch (JsonParseException e) {
            throw new FormatException(e.getMessage());
        }
    }

    @Override
    public Object replicate() throws IOException {
        return new JsonFormatParser();
    }

    @Override
    public boolean fill(RecordFIFO fifo) throws IOException, FormatException {
        if(jsonReader == null) {
            return false;
        }

        fifo.reset();

        while(true) {
            Record record = next();
            if(record == null) {
                if(fifo.byteSize() != 0) {
                    return true;
                } else {
                    jsonReader.close();
                    return false;
                }
            }

            fifo.add(record);
            if(fifo.byteSize() >= BUFFER_SIZE) {
                return true;
            }
        }
    }

    @Override
    public void initialize(FileQueue.LazyFile file) throws IOException {
    	InputStream is = file.getStream();
    	supportOuterCurley(is);
        jsonReader = new JsonReader(new InputStreamReader(is));
        jsonReader.beginArray();
    }

    /**
     * best effort attempt to support files of the format: { nameOfArrayOfRecords:[ ... ] }
     * 
     * @param is
     * @throws IOException
     */
    private void supportOuterCurley(InputStream is) throws IOException {
    	if (!is.markSupported()) return;
    	int MAX_SEARCH = 1000;
    	Pattern curleyPat = Pattern.compile("^\\{\"[\\w_]+\":");
    	
    	is.mark(MAX_SEARCH); // mark bof, limit search to 1000
    	
    	int pos;
    	StringBuffer sb = new StringBuffer();
    	for (pos = 0; pos < MAX_SEARCH; pos++) {
    		int ichar = is.read();
    		if (ichar == -1) break;
    		char ch = (char)ichar;
    		if (!Character.isWhitespace(ch)) {
    			sb.append(ch);
    		}

    		// break at first colon
    		if (ch == ':') break;
    	}

    	String beginning = sb.toString();
    	Matcher m = curleyPat.matcher(beginning);
    	if (m.matches()) {
    		is.reset();
    		is.skip(pos+1);
    		return;
    	}
    	
    	is.reset();
    }

    @Description(text={"Reads json file(s) from a directory.  Files should contain an unnamed json array with records as hashes. (see the json FormatWriter)",
    				   "A best-effort attempt will be made to parse files of the form: { nameOfArrayOfRecords:[ ... ] }" })
    @Param(name = JSON_LINES_PARAM, gloss = "File is in JSONlines format where each record is in its own line.", type = ArgType.BOOLEAN, defaultValue = "false")
   	public static class Op extends FormatOperator {
   		public Op() {
   			super(FORMAT, STREAM_FILE_REGEX);
   		}

   		@Override
   		public FormatParser getParser(SourceProperties props) throws IOException {
   		    Boolean jsonLines = (Boolean) props.getAccessArgs().getParam(JSON_LINES_PARAM);
   		    if(jsonLines)
   		        return new JsonLinesFormatParser();
   		    else
   			    return new JsonFormatParser();
   		}
   	}
}
