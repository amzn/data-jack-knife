package com.amazon.djk.keyed;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazon.djk.record.KeyMaker;
import com.amazon.djk.record.NotIterator;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.Fields;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.core.RecordSink;

/**
 * 
 *
 */
public abstract class KeyedSink extends RecordSink {
    private final NotIterator notKeys;
    protected final OpArgs args;
    protected final Fields keys;
    protected final KeyMaker keyMaker;
    protected int numMissingKeys = 0;

    /**
     * 
     * @param keyFields @param keyFields list of the fields in the incoming records to be used as the key for lookups.  All other fields become the payload.
     * @param keepers
     * @throws IOException 
     */
    public KeyedSink(RecordSink root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        this.keys = (Fields)args.getArg("KEYS");
        this.keyMaker = new KeyMaker(keys);
        notKeys = keys.getAsNotIterator();
    }
    
    /**
     * storeRecord creates defensive copies of a keyRecord and a valueRecord to be stored
     * by the sub-class.  Could parameterize this copying in the constructor args to avoid
     * copying when possible (often the sub-class will have to copy anyway).
     * 
     * @param rec
     * @throws IOException
     */
    protected void storeRecord(Record rec) throws IOException {
        Record valueRecord = new Record();
        notKeys.init(rec);
        while (notKeys.next()) {
        	valueRecord.addField(notKeys);
        }

        Record keyRecord = new Record();
        keyMaker.copyTo(rec, keyRecord);
        store(keyRecord, valueRecord);
        reportSunkRecord(1);
    }
    
    @Override
    public void drain(AtomicBoolean forceDone) throws IOException {
    	super.drain(forceDone);
        while (true) {
        	Record rec = next();
            if (rec == null || forceDone.get()) break;

            storeRecord(rec);
        }
    }

    public abstract void store(Record keyRecord, Record valueRecord) throws IOException;
}
