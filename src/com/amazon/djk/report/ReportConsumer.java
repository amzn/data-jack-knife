package com.amazon.djk.report;

import java.io.IOException;

/**
 * If a Sink implements ReportConsumer, the processor will call the consume method
 * after execution of the sink.  This offers the Sink the opportunity to persist
 * the report.  
 */
public interface ReportConsumer {
    void consume(ProgressReport report) throws IOException;
}
