package com.amazon.djk.format;

import com.amazon.djk.record.Record;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Sink records to a Json file
 *
 * @see {@JsonSerializer} for details
 */
public class JsonLinesFormatWriter extends FormatWriter {
    private final Gson gson;
    private PrintWriter writer = null;

    public JsonLinesFormatWriter(File dataFile) throws IOException {
        super(dataFile);
        writer = new PrintWriter(getStream());
        gson = new GsonBuilder().registerTypeAdapter(Record.class, new JsonSerializer()).create();
    }

    @Override
    public void writeRecord(Record rec) throws IOException {
        gson.toJson(rec, Record.class, writer);
        writer.println();
    }

    @Override
    public void close() throws IOException {
        writer.close();
        //super.close();
    }
}
