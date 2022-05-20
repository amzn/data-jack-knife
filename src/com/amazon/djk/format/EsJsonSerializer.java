package com.amazon.djk.format;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;


public class EsJsonSerializer extends JsonSerializer {

    @Override
    protected void addJsonObject(JsonObject jsonObject, String fieldName, JsonObject value) {
        if (jsonObject.has(fieldName)) {
            JsonElement element = jsonObject.get(fieldName);
            JsonArray array = generateJsonArrayIfDuplicate(element);
            array.add(value);
            jsonObject.add(fieldName, array);
        } else {
            jsonObject.add(fieldName, value);
        }
    }

}
