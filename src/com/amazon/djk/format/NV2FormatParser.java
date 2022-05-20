package com.amazon.djk.format;

import com.amazon.djk.core.MinimalRecordSource;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Record;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class NV2FormatParser extends ReaderFormatParser {
    public final static String STREAM_FILE_REGEX = "\\.nv2(\\.gz)?$";
    public static final String FORMAT = "nv2";
    private static final String END_OF_RECORD = "#";
    private final ArrayDeque<String> recLines = new ArrayDeque<>();
    private final List<Record> recs = new ArrayList<>();

    @Override
    public Record next(PushbackLineReader reader) throws IOException, FormatException {
        if (reader == null) return null;
        return parseRecord(reader);
    }

    private Record parseRecord(PushbackLineReader reader) throws IOException, FormatException {
        if (reader == null) return null;

        /*
         * recLines is loaded up with one record per call to parse() excluding #
         * it is allowable for the last record of the reader to end with empty lines instead
         * of the usually required #, otherwise empty lines are a format error.
         */
        recLines.clear();

        while (true) {
            String line = reader.readLine();

            if (line == null) {
                // allow missing END_OF_RECORD, if valid
                if (recLines.size() > 0) {
                    return parse(0, recLines);
                }

                return null; // we're done
            }

            // allow empty line as end of LAST record only,
            if (line.length() == 0) {
                if (endsWithAFewEmpties(reader)) {
                    if (recLines.size() > 0) {
                        return parse(0, recLines);
                    } else {
                        return null;
                    }
                } else {
                    throw new FormatException("blank lines only valid at end of file.");
                }
            }

            // END OF RECORD
            if (line.startsWith(END_OF_RECORD)) {
                if (recLines.size() > 0) {
                    return parse(0, recLines);
                } else {
                    continue;  // cause initial #'s to be eaten
                }
            }

            recLines.add(line);
        }
    }

    /**
     * EOF must come after a few empty lines
     *
     * @param reader
     * @return
     * @throws IOException
     */
    private boolean endsWithAFewEmpties(PushbackLineReader reader) throws IOException {
        for (int i = 0; i < 5; i++) {
            String line = reader.readLine();
            if (line == null) return true;
            if (line.length() != 0) return false;
        }

        return false;
    }

    /**
     * @param level
     * @param recLines
     * @return
     * @throws IOException
     */
    private Record parse(int level, ArrayDeque<String> recLines) throws IOException, FormatException {
        if (recs.size() <= level) {
            recs.add(new Record());
        }

        Record rec = recs.get(level);
        rec.reset();

        while (recLines.size() != 0) {
            String line = recLines.removeFirst();

            if (line.length() == 0) {
                throw new FormatException("empty line");
            }

            int numTabs = getNumInitialTabs(line);
            // if not a comment, where's the colon?
            int colon = line.indexOf(':');

            if (colon == -1) {
                throw new FormatException("missing colon");
            }

            // end of sub record
            if (numTabs < level) {
                if (level > 0) {
                    recLines.addFirst(line); // push back
                }

                if (rec.size() == 0) {
                    throw new FormatException("empty record");
                }

                return rec;
            }

            String name = line.substring(numTabs, colon);
            String value = line.substring(colon + 1);

            if (value.isEmpty()) { // either empty value or subrecord
                if (recLines.isEmpty()) {
                    // no more recLines
                    rec.addFieldTyped(name, value);
                    continue;
                }

                String next = recLines.removeFirst();
                if (getNumInitialTabs(next) == (level + 1)) {
                    recLines.addFirst(next);  // push back
                    Record sub = parse(level + 1, recLines);
                    rec.addField(name, sub);
                } else { // not subrecord must be zero length string.
                    recLines.addFirst(next);  // push back
                    rec.addFieldTyped(name, value);
                }
            } else {
                rec.addFieldTyped(name, value);
            }
        }

        return rec;
    }

    private int getNumInitialTabs(String line) {
        int numTabs = 0;
        while (numTabs < line.length() && line.charAt(numTabs) == '\t') {
            numTabs++;
        }

        return numTabs;
    }

    /**
     * utility function for getting small nv2 record sources directly from file
     *
     * @param file
     * @return
     */
    public static RecordSource getFromFile(File file) throws IOException {
        if (!file.exists()) {
            return null;
        }

        InputStream is = null;
        if (file.getName().endsWith(".gz")) {
            is = new FileInputStream(file);
            is = new GZIPInputStream(is, 32 * 1024);
            is = new BufferedInputStream(is, 3 * 1024);
        } else if (file.getName().endsWith(".bz2")) {
            is = new FileInputStream(file);
            is = new BZip2CompressorInputStream(is);
            is = new BufferedInputStream(is, 3 * 1024);
        } else { // not gzipped
            is = new FileInputStream(file);
            is = new BufferedInputStream(is, 3 * 1024);
        }

        Reader reader = new InputStreamReader(is);
        final PushbackLineReader pblr = new PushbackLineReader(reader);
        final NV2FormatParser parser = new NV2FormatParser();

        return new MinimalRecordSource() {
            @Override
            public Record next() throws IOException {
                try {
                    return parser.next(pblr);
                } catch (FormatException ee) {
                    throw new IOException("parse error", ee);
                }
            }

            @Override
            public void close() throws IOException {
                pblr.close(); // closes reader too
            }
        };
    }

    @Override
    public Object replicate() throws IOException {
        return new NV2FormatParser();
    }

    @Description(text = {"reads nv2 file(s) as a source of records."})
    public static class Op extends FormatOperator {
        public Op() {
            super(FORMAT, STREAM_FILE_REGEX);
        }

        @Override
        public FormatParser getParser(SourceProperties props) throws IOException {
            return new NV2FormatParser();
        }
    }
}
