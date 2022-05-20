package com.amazon.djk.stats;

import java.util.ArrayList;
import java.util.List;

/**                                                                                                                                 
 * Stores the value at request percentile. Uses Nearest Rank with rounding up.                                                      
 * https://w.amazon.com/index.php/WhatIsAPercentile                                                                                 
 *                                                                                                                                  
 */
public class Percentile<T extends Number> {
    private final List<PEntry<T>> entries = new ArrayList<>();
    private final long totalPoints;
    
    public static class PEntry<T extends Number> {
        public String label;
        public long percentileIndexAscending;
        public T value;
        
        public PEntry(String label, long percentileIndexAscending) {
            this.label = label;
            this.percentileIndexAscending = percentileIndexAscending;
        }
        
        public void setPercentile(T value, long numPoints ) {
            //use >= for round down method                                                                                              
            if (this.value == null && numPoints > percentileIndexAscending) {
                this.value = value;
            }
        }
    }

    public Percentile(long totalPoints) {
        this.totalPoints = totalPoints;
    }

    public List<PEntry<T>> getEntries() {
        return entries;
    }
    
    public void definePercentile(String label, double percentile) {
        long percentileIndexAscending = (long) (percentile * totalPoints);;
        entries.add(new PEntry<T>(label, percentileIndexAscending));
    }
    
    public void offerValue(T value, long totalPointsAscending) {
        for (PEntry<T> entry : entries) {
            entry.setPercentile(value, totalPointsAscending);
        }
    }
}
