package com.amazon.djk.stats;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.amazon.djk.record.Record;

public class StringFieldStats extends FieldStats<String> {
    private final Record data = new Record();
    
    public StringFieldStats(String fieldName, long maxPoints, long minPointCount) {
        super(fieldName, maxPoints, minPointCount);
    }

    @Override
    public Record getDataPointAsRecord(String value, AtomicLong count) throws IOException {
        data.reset();
        data.addField(StatFields.STATS_DATA_VALUE, value);
        data.addField(StatFields.STATS_DATA_COUNT, count.get());
        return data;
    }

	@Override
	public String fieldType() {
		return "String";
	}
}
