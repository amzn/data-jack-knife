package com.amazon.djk.source;

import java.io.IOException;
import java.util.List;

import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.report.ProgressData;
import com.amazon.djk.report.ReportFormats3;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;


@ReportFormats3(lineFormats ={"formatErrors=%,d versionId=%s"})
public class FileConsumerSource extends ConsumerSource {
	 @ScalarProgress(name="formatErrors", aggregate=AggType.NONE)
	    private long numFormatExceptions = 0;

	    @ScalarProgress(name="versionId", aggregate = AggType.NONE)
		private final String versionId;
			
	public FileConsumerSource(String url, List<RecordProducer> producers, SourceProperties props) throws IOException {
		super(url, producers);
		FileRecordProducer producer = (FileRecordProducer) producers.get(0);
		versionId = producer.getVersionId();
		reportTotalRecords(props.totalRecs());
	}

	private FileConsumerSource(FileConsumerSource root) throws IOException {
		super(root);
		versionId = root.versionId;
	}

	@Override
	public Object split() throws IOException {
		return new FileConsumerSource(this);
	}
	
	@Override
    public ProgressData getProgressData() {
        numFormatExceptions = 0;
        List<RecordProducer> producers = getProducers();
        for (RecordProducer producer : producers) {
            numFormatExceptions += ((FileRecordProducer)producer).getNumFormatExceptions();
        }

        return super.getProgressData();
    }
}
