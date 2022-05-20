package com.amazon.djk.stats;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.amazon.djk.record.Record;
import com.amazon.djk.stats.Percentile.PEntry;

public class LongFieldStats extends FieldStats<Long> {
    private final Record data = new Record();
    
    public LongFieldStats(String fieldName, long maxPoints, long minPointCount) {
        super(fieldName, maxPoints, minPointCount);
    }

    @Override
    public Record getDataPointAsRecord(Long value, AtomicLong count) throws IOException {
        data.reset();
        data.addField(StatFields.STATS_DATA_VALUE, value);
        data.addField(StatFields.STATS_DATA_COUNT, count.get());
        return data;
    }

    //@Override
    //public int comparePoints(Long a, Long b) {
     //   return a.compareTo(b);
    //}
    
    @Override
    public Record getNumericalStatsAsRecord() throws IOException {
        Record out = new Record();
        
        Percentile<Long> percentiles = new Percentile<Long>(numValues.get());
        percentiles.definePercentile(StatFields.STATS_P10, 0.1);
        percentiles.definePercentile(StatFields.STATS_P50, 0.5);
        percentiles.definePercentile(StatFields.STATS_P90, 0.9);
        percentiles.definePercentile(StatFields.STATS_P99, 0.99);
        percentiles.definePercentile(StatFields.STATS_P999, 0.999);
        
        //increasing value order
        List<Long> valueAscending = Collections.list(items.keys());
        Collections.sort(valueAscending, new Comparator<Long>() {
            public int compare(Long a, Long b) {
                return Long.compare(a, b);
            }
        });
        
        LongSummaryStatistics summaryStats = (LongSummaryStatistics) valueAscending.stream().collect(Collectors.summarizingLong(Long::longValue));
        double doubleSum = items.entrySet().stream().mapToLong(e -> e.getKey() * e.getValue().get()).sum();

        double doubleAve = doubleSum / (double)numValues.get();
        double sumDiffSq = 0.0F;
        long totalPoints = 0;

        for (Long value : valueAscending) {
            AtomicLong count = items.get(value);
            double diffSq = (value - doubleAve) * (value - doubleAve);
            sumDiffSq += diffSq * count.get();
            totalPoints += count.get();
            percentiles.offerValue(value, totalPoints);
        }

        double doubleStdev = Math.sqrt(sumDiffSq / numValues.get());
        out.addField(StatFields.STATS_SUM, doubleSum);
        out.addField(StatFields.STATS_MAX, summaryStats.getMax());
        out.addField(StatFields.STATS_MIN, summaryStats.getMin());
        out.addField(StatFields.STATS_AVE,doubleAve);
        out.addField(StatFields.STATS_STDEV, doubleStdev);
        
        List<PEntry<Long>> entries = percentiles.getEntries();
        for (PEntry<Long> entry : entries) {
            out.addField(entry.label, entry.value);
        }

        return out;
    }

	@Override
	public String fieldType() {
		return "Long";
	}
}
