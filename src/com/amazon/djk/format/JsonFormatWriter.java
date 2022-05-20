package com.amazon.djk.format;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.Param;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Record;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

/**
 * Sink records to a Json file
 * @see {@JsonSerializer} for details
 */
public class JsonFormatWriter extends FormatWriter {
	private final static String JSON_LINES_PARAM = "jsonLines";
	public final static String FORMAT = "json";
    private final Gson gson;
    private JsonWriter writer = null;

    public JsonFormatWriter(File dataFile) throws IOException {
    	super(dataFile);
        writer = new JsonWriter(new PrintWriter(getStream()));
        gson = new GsonBuilder().registerTypeAdapter(Record.class, new JsonSerializer()).create();
        writer.beginArray();
	}
    
    @Override
	public void writeRecord(Record rec) throws IOException {
        gson.toJson(rec, Record.class, writer);
	}

	@Override
	public void close() throws IOException {
        writer.endArray();
        writer.close();
        //super.close();
	}

    @Description(text = {"Writes records as json file(s).  Files will contain an unnamed json array with records as hashes.",
			"E.g: [{\"id\":2,\"hello\":\"there\"},{\"id\":1,\"hello\":\"world\"}]",
    		"Subrecords are represented as a named array within a record.",
    		"E.g: [{\"id\":1,\"fruit\":[{\"type\":\"berry\",\"color\":\"red\"},{\"type\":\"citrus\",\"color\":\"yellow\"}]}]"})
	@Param(name = JSON_LINES_PARAM, gloss = "File is in JSONlines format where each record is in its own line.", type = ArgType.BOOLEAN, defaultValue = "false")
	public static class Op extends WriterOperator {
    	public Op() {
			super("json", JsonFormatParser.STREAM_FILE_REGEX);
		}

		@Override
		public FormatWriter getWriter(FormatArgs args, File dataFile) throws IOException {
    	    Boolean jsonLines = (Boolean) args.getParam(JSON_LINES_PARAM);
    	    if(jsonLines)
    	    	return new JsonLinesFormatWriter(dataFile);
    	    else
				return new JsonFormatWriter(dataFile);
		}
    }
}
