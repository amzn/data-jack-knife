package com.amazon.djk.format;

import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Record;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigDecimal;

/**
 * Serialize record to a Json string
 *
 * Records support values with the same key while Json does not. Therefore if there are duplicate keys, a JsonArray
 * is formed
 *
 * e.g
 *
 * key:obj1
 * key:obj2
 * anotherKey:randomObj
 * key:obj3
 * #
 *
 * maps to:
 *
 * {{"key":[obj1,obj2,obj3], "anotherKey":randomObj}}
 *
 */
public class JsonSerializer implements com.google.gson.JsonSerializer<Record> {
    @Override
    public JsonElement serialize(Record src, Type typeOfSrc, JsonSerializationContext context) {
        try {
			return serializeRecord(src, 0);
		} catch (IOException e) {
			return null;
		}
    }

    private JsonObject serializeRecord(Record record, int level) throws IOException {
        FieldIterator fields = new FieldIterator();
        JsonObject jsonRecord = new JsonObject();
        fields.init(record);

        while (fields.next()) {
            String fieldName = fields.getName();
            switch (fields.getType()) {
                case RECORD:
                    Record subrec = new Record();
                    fields.getValueAsRecord(subrec);
                    JsonObject subRecordJson = serializeRecord(subrec, level + 1);
                    addJsonObject(jsonRecord, fieldName, subRecordJson);
                    break;
                case LONG:
                    addPrimitive(jsonRecord, fieldName, fields.getValueAsLong());
                    break;
                case DOUBLE:
                    addPrimitive(jsonRecord, fieldName, fields.getValueAsDouble());
                    break;
                case STRING:
                    addPrimitive(jsonRecord, fieldName, fields.getValueAsString());
                    break;
                case BOOLEAN:
                    addPrimitive(jsonRecord, fieldName, fields.getValueAsBoolean());
                    break;
                default:
                    addJsonNull(jsonRecord, fieldName);
            }
        }
        
        return jsonRecord;
    }

    protected void addJsonObject(JsonObject jsonObject, String fieldName, JsonObject value) {
        JsonArray array;
        if (jsonObject.has(fieldName)) {
            JsonElement element = jsonObject.get(fieldName);
            array = generateJsonArrayIfDuplicate(element);
        } else {
            array = new JsonArray();
        }
        array.add(value);
        jsonObject.add(fieldName, array);
    }

    private void addJsonNull(JsonObject jsonObject, String fieldName) {
        if (jsonObject.has(fieldName)) {
            JsonElement element = jsonObject.get(fieldName);
            JsonArray array = generateJsonArrayIfDuplicate(element);
            array.add(JsonNull.INSTANCE);
            jsonObject.add(fieldName, array);
        } else {
            jsonObject.add(fieldName, JsonNull.INSTANCE);
        }
    }

    private void addPrimitive(JsonObject jsonObject, String fieldName, Object value) {
        if (jsonObject.has(fieldName)) {
            JsonElement element = jsonObject.get(fieldName);
            if(!element.isJsonArray() && !element.isJsonPrimitive()) {
                throw new IllegalStateException("Json element should be either a Json array or a Json primitive");
            }
            JsonArray array = generateJsonArrayIfDuplicate(element);
            array.add(createJsonPrimitive(value));
            jsonObject.add(fieldName, array);
        } else {
            jsonObject.add(fieldName, createJsonPrimitive(value));
        }
    }

    protected JsonArray generateJsonArrayIfDuplicate(JsonElement element) {
        if (element.isJsonArray()) {
            return element.getAsJsonArray();
        } else {
            JsonArray array = new JsonArray();
            array.add(element);
            return array;
        }
    }

    private JsonElement createJsonPrimitive(Object value) {
        if (value instanceof Number) {
            // Turn off scientific notation for Double
            return new JsonPrimitive(value instanceof Double ? BigDecimal.valueOf((Double) value) : (Number) value);
        }

        if (value instanceof String) {
            return new JsonPrimitive((String) value);
        }

        if (value instanceof Boolean) {
            return new JsonPrimitive((Boolean) value);
        }

        if (value instanceof Character) {
            return new JsonPrimitive((Character) value);
        }

        throw new IllegalStateException(String.format("Type %s is not supported in Json primitive", value.getClass().getSimpleName()));
    }

}
