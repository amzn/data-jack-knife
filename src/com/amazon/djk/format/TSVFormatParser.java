package com.amazon.djk.format;

import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.Param;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

public class TSVFormatParser extends ReaderFormatParser {
    private final static Logger LOGGER = LoggerFactory.getLogger(TSVFormatParser.class);
    private final static String NOEMPTY_PARAM = "noEmpty";
    public final static String FIELDS_PARAM = "fields";
    public final static String DELIM_PARAM = "delim";
    public final static String STREAM_FILE_REGEX = "\\.tsv(\\.gz|\\.bz2)?$";
    public final static  String FORMAT = "tsv";
    public final static String DEFAULT_DELIMITER = "\t";

    private final String valueSplitRegex;
    private final Fields fields;
    private final Boolean noEmpty;

    private final Record rec = new Record();
    private String[] fieldNames = null;
    private long lineNumber = 0;

    /**
     * root constructor
     *
     * @param args
     * @throws IOException
     */
    public TSVFormatParser(FormatArgs args) throws IOException {
        String delim = (String) args.getParam(DELIM_PARAM);
        // converting the delimiter to an actual regex so that we handle regex escape characters -
        // eg. \t => \Q    \E
        this.valueSplitRegex = Pattern.quote(delim);
        this.fields = (Fields) args.getParam(FIELDS_PARAM);
        this.noEmpty = (Boolean) args.getParam(NOEMPTY_PARAM);
    }

    /**
     * replicate constructor
     *
     * @param root
     * @throws IOException
     */
    public TSVFormatParser(TSVFormatParser root) throws IOException {
        this.valueSplitRegex = root.valueSplitRegex;
        this.fields = root.fields;
        this.noEmpty = root.noEmpty;
    }

    @Override
    public void initialize(PushbackLineReader reader) throws IOException {
        if (fields != null) { // fields param?
            fieldNames = fields.getFieldNames().toArray(new String[] {});
        }

        else if (reader != null) {
            // field names are written in the first line of the file
            lineNumber++;
            // sometimes people put spaces in field names (who are these people?!)
            String headerLine = reader.readLine().replaceAll(" ", "_");
            if (Strings.isNullOrEmpty(headerLine)) {
                return;
            }
            fieldNames = headerLine.split(valueSplitRegex);
        }
    }

    @Override
    public Record next(PushbackLineReader reader) throws FormatException, IOException {
        if(reader == null) {
            return null;
        }

        lineNumber++;
        rec.reset();
        while (true){
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            if (line.isEmpty()) {
                continue;
            }

            // -1, keep space at end
            String[] values = line.split(valueSplitRegex, -1);
            if (values.length != fieldNames.length) {
                throw new FormatException(String.format("lineNumber=%d contains %d columns, header contains %d", lineNumber, values.length, fieldNames.length));
            }

            for (int i = 0; i < fieldNames.length; i++) {
                if (!noEmpty || values[i].length() != 0) {
                    rec.addFieldTyped(fieldNames[i], values[i]);
                }
            }
            
            return rec;
        }
    }

    @Override
    public Object replicate() throws IOException {
        return new TSVFormatParser(this);
    }

    @Description(text = { "reads tsv file(s) as a source of records." })
    @Param(name = FIELDS_PARAM, gloss = "Comma separated list of fields to be used as the header for the file."
            + " If set, the first line of the file will be treated as data."
            + " If not set, the first line of the file will be treated as headers."
            + " Spaces in field names will be replace with underbars.", type = ArgType.FIELDS)
    @Param(name = DELIM_PARAM, gloss = "String to be used as delimiter.", type = ArgType.STRING, defaultValue = DEFAULT_DELIMITER)
    @Param(name = NOEMPTY_PARAM, gloss = "Ignores zero length string fields.", type = ArgType.BOOLEAN, defaultValue = "false")
    public static class Op extends FormatOperator {
        public Op() {
            super(FORMAT, STREAM_FILE_REGEX);
        }

        @Override
        public FormatParser getParser(SourceProperties props) throws IOException {
            FormatArgs args = props.getAccessArgs();
            return new TSVFormatParser(args);
        }
    }
}
