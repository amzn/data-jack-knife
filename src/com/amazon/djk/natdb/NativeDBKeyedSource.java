package com.amazon.djk.natdb;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.format.FormatOperator;
import com.amazon.djk.format.FormatParser;
import com.amazon.djk.keyed.KeyedSource;
import com.amazon.djk.keyed.OuterKeyedSource;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.KeyMaker;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;

@ReportFormats2(headerFormat="<args>%s?keys=%s",
	lineFormats={"numRecs=%,d"}
)
public class NativeDBKeyedSource extends OuterKeyedSource {
    private static final String FORMAT = "natdb";
    private final File dbDir;
    private final BucketAccessSet bucketAccess;
    private final int numBuckets;
    private final DiskEntryDecoder decoder;
    
    @ScalarProgress(name="args")
    private final FormatArgs accessArgs;
    
    @ScalarProgress(name="keys")
    private final String keys;
    
    @ScalarProgress(name="numRecs")
    private final long totalRecs;

    private final KeyMaker keyMaker;
    private final Record keyRecord = new Record();

    public NativeDBKeyedSource(OpArgs args) throws IOException {
        super(args, getKeyFields(args));
        
        accessArgs = (FormatArgs)args;
        dbDir = new File(accessArgs.getPath());
        SourceProperties props = accessArgs.getSourceProperties();
        totalRecs = props.totalRecs();
        
        String temp = props.getExtra("numBuckets");
        numBuckets = Integer.parseInt(temp);
        decoder = new DiskEntryDecoder(props);
        bucketAccess = BucketAccessSet.create(dbDir, numBuckets, decoder);
        
        keys = props != null ? StringUtils.join(keyFieldNames, ",") : "";
        keyMaker = new KeyMaker(getKeyFieldNames());
    }
    
    public NativeDBKeyedSource(OpArgs args, BucketAccessSet bucketAccess) throws IOException {
        super(args, getKeyFields(args));
        accessArgs = (FormatArgs)args;
        dbDir = new File(accessArgs.getPath());
        SourceProperties props = accessArgs.getSourceProperties();
        String temp = props.getExtra("numBuckets");
        numBuckets = Integer.parseInt(temp);
        decoder = new DiskEntryDecoder(props);
        totalRecs = props.totalRecs();
        
        keys = props != null ? StringUtils.join(keyFieldNames, ",") : "";
        this.bucketAccess = bucketAccess;
        keyMaker = new KeyMaker(getKeyFieldNames());
    }
    
    private static String[] getKeyFields(OpArgs args) throws IOException {
        FormatArgs access = (FormatArgs)args;
        SourceProperties props = access.getSourceProperties();
        return props.getKeyFields();
    }
    
    @Override
    public long getNumRecords() {
    	return totalRecs;
    }
    

    @Override
    public Object replicateKeyed() throws IOException {
        return new NativeDBKeyedSource(args, bucketAccess.replicate());
    }
    
    @Override
    public Record getValue(Record lookupRecord) throws IOException {
        keyRecord.reset();
        keyMaker.copyTo(lookupRecord, keyRecord);
        return bucketAccess.getValue(keyRecord, false);
    }
    
    @Override
    public Record getValueOnce(Record lookupRecord) throws IOException {
        keyRecord.reset();
        keyMaker.copyTo(lookupRecord, keyRecord);
    	return bucketAccess.getValue(keyRecord, true);
    }

    @Override
	public void enableOuterAccess() {
    	bucketAccess.enableOuterAccess();
	}

	public void prepareOuterAccess() {
    	bucketAccess.prepareOuterAccess();
    }
    
    @Override
    public Record next() throws IOException {
    	return bucketAccess.next();
    }
    
    @Description(text={"Allows streaming or joining of records by the keys."},
            contexts={"PATH ...", "... PATH join"})
    public static class Op extends FormatOperator {
        public Op() {
            super(FORMAT, NativeDBFileParser.STREAM_FILE_REGEX);
        }
        
        @Override
        public FormatParser getParser(SourceProperties props) throws IOException {
            validateArgs(props.getAccessArgs());
            return new NativeDBFileParser(props);
        }
        
        @Override
        public KeyedSource getKeyedSource(OpArgs accessArgs) throws IOException {
            validateArgs(accessArgs);
            return new NativeDBKeyedSource(accessArgs);
        }
        
        private void validateArgs(OpArgs accessArgs) throws IOException {
            FormatArgs fargs = (FormatArgs)accessArgs;
            SourceProperties props = fargs.getSourceProperties();
            // if no source props this is zero len
            if (props.getKeyFields().length < 1) {
                throw new SyntaxError(fargs.getPath() + " not valid native DB path");
            }
        }
    }
}
