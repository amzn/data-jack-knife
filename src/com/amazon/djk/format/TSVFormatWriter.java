package com.amazon.djk.format;

import static com.amazon.djk.format.TSVFormatParser.DELIM_PARAM;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.amazon.djk.expression.ArgType;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.FieldType;
import com.amazon.djk.record.Record;

public class TSVFormatWriter extends FormatWriter {
	private final PrintWriter writer;
	private List<String> fieldNames = null;
    protected final String delim;
	
	public TSVFormatWriter(String delim, File dataFile) throws IOException {
		super(dataFile);
		writer = new PrintWriter(getStream());
		this.delim = delim;
	}
	
	private void writeHeader(Record rec) throws IOException {
		fieldNames = getFieldNames(rec);
        Iterator<String> it = fieldNames.iterator();
        writer.print(it.next());
        while (it.hasNext()) {
            writer.print(delim);
            writer.print(it.next());
        }
        writer.println();
	}
	
	private List<String> getFieldNames(Record rec) throws IOException {
        FieldIterator fieldIt = new FieldIterator();
        Set<String> fieldsSet = new HashSet<>();
        fieldIt.init(rec);
        while (fieldIt.next()) {
            if (fieldIt.getType() == FieldType.RECORD) { // skipping subrecords
                continue;
            }
            
            if (fieldsSet.contains(fieldIt.getName())) {
                throw new IOException("field: " + fieldIt.getName() + " appears multiple times in the record.");
            }
            fieldsSet.add(fieldIt.getName());
        }
        List<String> fieldNames = new ArrayList<>(fieldsSet);
        
        if(fieldNames.isEmpty()) {
            throw new IOException("the record does not contain any non-record type fields");
        }
        
        return fieldNames;
    }

	@Override
	public void writeRecord(Record rec) throws IOException {
		if (fieldNames == null) { // field names are written in the first line
			writeHeader(rec);
		}

        Iterator<String> it = fieldNames.iterator();
        writer.print(rec.getFirstAsString(it.next()));
        while (it.hasNext()) {
            writer.print(delim);
            writer.print(rec.getFirstAsString(it.next()));
        }

        writer.println();
	}
	
	@Override
	public void close() {
		writer.close();
	}
	
	
	@Description(text={"Writes records as tsv file(s)."})
    @Param(name="delim", gloss="Specifies the delimiter to be used to separate the data", type = ArgType.STRING, defaultValue = TSVFormatParser.DEFAULT_DELIMITER)
	public static class Op extends WriterOperator {
		public Op() {
			super("tsv", TSVFormatParser.STREAM_FILE_REGEX);
		}

		@Override
		public FormatWriter getWriter(FormatArgs args, File dataFile) throws IOException {
			String delim = (String) args.getParam(DELIM_PARAM);
			return new TSVFormatWriter(delim, dataFile);
		}
	}
}
