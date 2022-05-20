package com.amazon.djk.record;

import java.util.AbstractMap;
import java.util.Map;

public class Pair extends AbstractMap.SimpleImmutableEntry<Field, Value> {

    public Pair(Field key, Value value) {
        super(key, value);
    }

    public static Pair of(Field key, Value value) {
        return new Pair(key, value);
    }

    public Pair(Map.Entry<? extends Field, ? extends Value> entry) {
        super(entry);
    }

    public static Pair of(Map.Entry<? extends Field, ? extends Value> entry) {
        return new Pair(entry);
    }

}
