package com.amazon.djk.format;

import com.amazon.djk.record.Record;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;

import java.io.IOException;

public class JsonLinesFormatParser extends ReaderFormatParser {
    private final Gson gson;

    public JsonLinesFormatParser() {
        gson = new GsonBuilder().registerTypeAdapter(Record.class, new JsonDeserializer()).create();
    }

    @Override
    public Object replicate() throws IOException {
        return new JsonLinesFormatParser();
    }

    @Override
    public Record next(PushbackLineReader reader) throws IOException, FormatException {
        if (reader == null) {
            return null;
        }

        while (true) {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            if (line.isEmpty()) {
                continue;
            }

            try {
                return gson.fromJson(line, Record.class);
            } catch (JsonParseException e) {
                throw new FormatException(e.getMessage());
            }
        }


    }
}
