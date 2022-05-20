package com.amazon.djk.stats;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.record.Record;

/**
 * thread-safe  
 */
public class StatsHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatsHolder.class);

    private final AtomicLong numRecs = new AtomicLong(0);
    private final ConcurrentHashMap<String, FieldStats<?>> statsMap = new ConcurrentHashMap<>();
    private final long maxPoints;
    private final long minPointCount;
    

    
    public StatsHolder(long maxPoints, long minPointCount) throws IOException {
        this.maxPoints = maxPoints;
        this.minPointCount = minPointCount;
    }
    
    public void incNumRecs() {
        numRecs.incrementAndGet();
    }
    
    /**
     * 
     * @param fieldName
     * @param value
     */
    public void addString(String fieldName, String value) {
        statsMap.compute(fieldName, (k,v) -> {
            StringFieldStats map = v == null ? new StringFieldStats(fieldName, maxPoints, minPointCount) : (StringFieldStats) v;
             map.count(value);
             return map;
        });
    }

    /**
     * 
     * @param fieldName
     * @param value
     */
    public void addLong(String fieldName, Long value) {
        statsMap.compute(fieldName, (k, v) -> {
            LongFieldStats map = v == null ? new LongFieldStats(fieldName, maxPoints, minPointCount) : (LongFieldStats) v;
            map.count(value);
            return map;
        });
    }

    /**
     * 
     * @param fieldName
     * @param value
     */
    public void addDouble(String fieldName, Double value) {
        statsMap.compute(fieldName, (k, v) -> {
            DoubleFieldStats map = v == null ? new DoubleFieldStats(fieldName, maxPoints, minPointCount) : (DoubleFieldStats) v;
            map.count(value);
            return map;
        });
    }

    /**
     * 
     * @param fieldName
     * @param value
     */
    public void addBoolean(String fieldName, Boolean value) {
        statsMap.compute(fieldName, (k, v) -> {
            BooleanFieldStats map = v == null ? new BooleanFieldStats(fieldName, maxPoints, minPointCount) : (BooleanFieldStats) v;
            map.count(value);
            return map;
        });
    }

    /**
     * the child expression level reduction
     * 
     * @return
     * @throws IOException
     */
    public Record getChildReduction(String statInstance) throws IOException {
        Record rec = new Record();
        statsMap.forEach((key, value) -> {
            try {
                rec.addField(StatFields.STATS_CHILD_NAME, value.getAsRecord(numRecs.get(), statInstance));
            } catch (IOException ex) {
                LOGGER.error("Caught IOException while parsing stats.", ex);
            }
        });
        
        return rec;
    }
    
    /**
     * the main expression level reduction
     * 
     * @return
     * @throws IOException
     */
    public Record getNextMainReduction(String statInstance) throws IOException {
        Iterator<FieldStats<?>> fieldStatsIterator = statsMap.values().iterator();
        while (fieldStatsIterator.hasNext()) {
            Record statsRec = fieldStatsIterator.next().getAsRecord(numRecs.get(), statInstance);
            fieldStatsIterator.remove();
            return statsRec;
        }
    	return null;
    }
}
