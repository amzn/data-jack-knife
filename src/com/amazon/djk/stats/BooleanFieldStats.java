package com.amazon.djk.stats;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.amazon.djk.record.Record;

public class BooleanFieldStats extends FieldStats<Boolean> {
    private final Record data = new Record();
    
    public BooleanFieldStats(String fieldName, long maxPoints, long minPointCount) {
        super(fieldName, maxPoints, minPointCount);
    }

    @Override
    public Record getDataPointAsRecord(Boolean value, AtomicLong count) throws IOException {
        data.reset();
        data.addField(StatFields.STATS_DATA_VALUE, value);
        data.addField(StatFields.STATS_DATA_COUNT, count.get());
        return data;
    }

    //@Override
    //public int comparePoints(Boolean a, Boolean b) {
     //   return a.compareTo(b);
    //}

	@Override
	public String fieldType() {
		return "Boolean";
	}
}
