package com.amazon.djk.format;

import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Record;
import com.google.common.base.Strings;

import java.io.IOException;

public class LineFormatParser extends ReaderFormatParser {
    public final static String FORMAT = "txt";
    public final static String STREAM_FILE_REGEX = "\\.txt(\\.gz)?$";
    private final Record rec = new Record();
	
    @Override
    public Record next(PushbackLineReader reader) throws IOException {
        if (reader == null) return null;
        
        String line = reader.readLine();
        if (Strings.isNullOrEmpty(line)) {
            return null;
        }
        
        rec.reset();
        rec.addField(FORMAT, line.trim());
        return rec;
    }

    @Override
    public Object replicate() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Description(text = { "reads *.txt file(s) as a source of records" })
    public static class Op extends FormatOperator {
        public Op() {
            super(FORMAT, STREAM_FILE_REGEX);
        }

        @Override
        public FormatParser getParser(SourceProperties props) throws IOException {
            return new LineFormatParser();
        }
    }
}
