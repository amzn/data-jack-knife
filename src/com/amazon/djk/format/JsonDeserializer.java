package com.amazon.djk.format;

import com.amazon.djk.record.Record;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang.Validate;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Iterator;

public class JsonDeserializer implements com.google.gson.JsonDeserializer<Record> {

    @Override
    public Record deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        Validate.isTrue(json.isJsonObject(), "JsonElement for JsonFormatParser should be a JsonObject");

        Record rec = new Record();
        parseJsonObjectAndAddToRecord(rec, json.getAsJsonObject());
        return rec;
    }

    private void parseJsonObjectAndAddToRecord(Record record, JsonObject value) {
        value.getAsJsonObject().entrySet().stream()
                .forEach(entry -> {
                    try {
                        parseJsonElementAndAddToRecord(record, entry.getKey(), entry.getValue());
                    } catch (IOException e) {
                        throw new JsonParseException(e);
                    }
                });
    }

    private void parseJsonElementAndAddToRecord(Record record, String fieldName, JsonElement value) throws IOException {
        if (value.isJsonArray()) {
            parseJsonArrayAndAddToRecord(record, fieldName, value.getAsJsonArray());
            return;
        }

        if (value.isJsonObject()) {
            Record rec = new Record();
            parseJsonObjectAndAddToRecord(rec, value.getAsJsonObject());
            record.addField(fieldName, rec);
            return;
        }

        if (value.isJsonPrimitive()) {
            parseJsonPrimitiveAndAddToRecord(record, fieldName, value.getAsJsonPrimitive());
            return;
        }

    }

    private void parseJsonArrayAndAddToRecord(Record record, String fieldName, JsonArray value) throws IOException {
        Iterator<JsonElement> iterator = value.iterator();
        while (iterator.hasNext()) {
            parseJsonElementAndAddToRecord(record, fieldName, iterator.next());
        }
    }

    private void parseJsonPrimitiveAndAddToRecord(Record record, String fieldName, JsonPrimitive value) throws IOException {
        record.addFieldTyped(fieldName, value.getAsString());
    }
}
