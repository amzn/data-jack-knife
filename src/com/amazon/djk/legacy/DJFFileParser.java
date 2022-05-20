package com.amazon.djk.legacy;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Stack;

import com.amazon.djk.file.FileQueue.LazyFile;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.format.FormatException;
import com.amazon.djk.format.FormatOperator;
import com.amazon.djk.format.FormatParser;
import com.amazon.djk.format.PushbackLineReader;
import com.amazon.djk.format.ReaderFormatParser;
import com.amazon.djk.format.FileFormatParser;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;
import com.amazon.djk.record.RecordIO.IOBytes;
import com.amazon.djk.record.ThreadDefs;

public class DJFFileParser extends FileFormatParser {
    public final static String STREAM_FILE_REGEX = "\\.djf$";
    public static final String FORMAT = "djf";
    private final SourceProperties props;
    private Record rec1;
    private final String[] keyFields;
    private final IOBytes keyBytes = new IOBytes();
    private final IOBytes compressedBytes = new IOBytes();
    private final IOBytes recBytes = new IOBytes();
    private final NVPFormatParser nvpParser;
    private final Stack<String> lineStack = new Stack<>();
    private final String EOR = "---:";
    
    private DataInputStream stream = null;

    public DJFFileParser(SourceProperties props, String[] keys) throws IOException {
        this.props = props;
        keyFields = keys;
        nvpParser = new NVPFormatParser(props, false); // don't require whitelists
    }

    @Override
    public Object replicate() throws IOException {
        return new DJFFileParser(props, keyFields);
    }
    
    @Override
    public void initialize(LazyFile file) throws IOException {
        stream = new DataInputStream(file.getStream());
        stream.readByte(); // data starts at 1        
    }
    
    @Override
    public boolean fill(RecordFIFO fifo) throws IOException, FormatException {
        if (stream == null) return false;

        fifo.reset();
        do {
            if (!setRec1(stream)) {
                break;
            }

            fifo.add(rec1);
        } while (fifo.byteSize() < 1024 * 64);

        return fifo.byteSize() != 0;
    }

    /**
     * parse the stream format like an nvp record.
     * 
     * @param stream
     * @param out
     * @return true if a record was read
     * @throws IOException
     * @throws FormatException 
     */
    private boolean setRec1(DataInputStream stream) throws IOException, FormatException {
        try {
            keyBytes.reset();
            compressedBytes.reset();            
            recBytes.reset();
            
            if (keyFields != null) {
                int keyLen = stream.readInt();
                if (!keyBytes.read(stream, keyLen)) {
                    return false;
                }
            }

            int recLen = stream.readInt();
            if (!compressedBytes.read(stream, recLen)) {
                return false;
            }

            // legacy inflation
            ThreadDefs.get().inflate(compressedBytes, recBytes, false);
            String rec = new String(recBytes.getAsByteArray(), "UTF-8");
            String[] lines = rec.split("\\n"); 
            
            lineStack.clear();
            lineStack.add(EOR);
            for (String line : lines) {
                lineStack.add(line);
            }
            
            PushbackLineReader lr = new PushbackLineReader(lineStack);
            rec1 = nvpParser.next(lr);
            
            if (keyFields != null) {
                String keys = new String(keyBytes.getAsByteArray(), "UTF-8");
                String[] keyValues = keys.split("\\|", -1); // -1 so "foo|" returns 2 elems not 1
                
                if (keyValues.length != keyFields.length) {
                	throw new IOException("bad key field data:" + keys);
                }

                for (int i = 0; i < keyFields.length; i++) {
                    ReaderFormatParser.addPrimitiveValue(rec1, keyFields[i], keyValues[i]);
                }
            }
        }

        catch (EOFException e) {
            return false;
        }

        return true;
    }

    @Description(text = { "Allows streaming of a djf legacy source." })
    public static class Op extends FormatOperator {
        public final static byte DELIMITER = (byte) '|';
        public final static String KEYS_PROP = "keys";
        public final static String NUM_RECORDS_PROP = "numRecords";

        public Op() {
            super(FORMAT, STREAM_FILE_REGEX);
        }

        @Override
        public FormatParser getParser(SourceProperties props) throws IOException {
            FormatArgs args = props.getAccessArgs();
            String path = args.getPath();

            // read the legacy props
            File file = new File(path, "djf.properties");
            Properties djfprops = new Properties();
            djfprops.load(new FileInputStream(file));

            String keysString = djfprops.getProperty(KEYS_PROP);
            String[] keys = keysString != null ? keysString.split(",") : null;

            return new DJFFileParser(props, keys);
        }
    }
}
