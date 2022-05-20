package com.amazon.djk.natdb;

import java.io.IOException;

import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordIO;
import com.amazon.djk.record.RecordIO.Direction;
import com.amazon.djk.record.RecordIO.IOBytes;
import com.amazon.djk.record.RecordIO.IORecord;
import com.amazon.djk.record.ThreadDefs;

/**
 * Decoder translates from the on disk record:
 * [keyFields][compressedValueField]
 * 
 * to a useable record.  This translation occcurs before the RecordIO.translation of
 * field Ids happens.  For that reason (i.e. we don't have Global field IDs) we 
 * count the number of fields instead of looking for them by ID.
 *
 */
public class DiskEntryDecoder {
	private final SourceProperties props;
    private final boolean onePerKey;
    private final IOBytes compressedBytes = new IOBytes();
    private final int numKeyFields;
    private final FieldIterator fiter = new FieldIterator();
    private final RecordIO recordIO;
    
    public DiskEntryDecoder(SourceProperties props) throws IOException {
    	this.props = props;
        numKeyFields = props.getKeyFields().length;
        onePerKey = props.getExtra("groupOut").equals("NONE");
        recordIO = new RecordIO(props.getSourceFields());
    }
    
    public DiskEntryDecoder replicate() throws IOException {
    	return new DiskEntryDecoder(props);
    }
    
    /**
     * 
     * @return the non-thread safe instance of the field translator
     */
    public RecordIO getRecordIO() {
        return recordIO;
    }
    
    public void decode(IORecord inrec, Record outrec, boolean withKey) throws IOException {
        outrec.reset();
        
        fiter.init(inrec);
        for (int i = 0; i < numKeyFields; i++) {
            if (!fiter.next()) {
                throw new IOException("format error missing key field number=" + i);
            }

            if (withKey) {
                outrec.addField(fiter);
            }
        }
        
        // get out the compressed value field

        // no grouping
        if (onePerKey) {
            if (!fiter.next()) {
                throw new IOException("format error missing value data");
            }

            compressedBytes.reset();
            // TODO: this method is broken to accommodate something broken here! FIX ME
            fiter.getValueAsBytesBROKENnatdb(compressedBytes);
            ThreadDefs.get().inflate(compressedBytes, outrec);
        }
    
        else { // else grouped
            if (!fiter.next()) {
                throw new IOException("format error missing value data");
            }

            compressedBytes.reset();
            // TODO: this method is broken to accommodate something broken here! FIX ME
            fiter.getValueAsBytesBROKENnatdb(compressedBytes);
            ThreadDefs.get().inflate(compressedBytes, outrec);
        }
        
        recordIO.translate(outrec, Direction.STORED_TO_LIVE);
    }
}
