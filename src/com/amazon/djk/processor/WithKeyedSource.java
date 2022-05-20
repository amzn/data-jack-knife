package com.amazon.djk.processor;

import com.amazon.djk.core.RecordSource;

/*
 * Predicates that implement WithKeyedSource expect a left and a right source like:
 * djk leftSource predicate1 predicate2 rightKeyedSource withKeyedSourcePredicate
 * 
 * e.g. 'join'
 * 
 */
public interface WithKeyedSource {

    /**
     * 
     * @return the keyedSource 
     */
    RecordSource getKeyedSource();
}
