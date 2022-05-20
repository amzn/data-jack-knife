package com.amazon.djk.core;

import java.io.IOException;

import com.amazon.djk.record.Record;
import com.amazon.djk.core.RecordPipe;

/**
 * This class implements a kind of Broadcast functionality.  Each
 * subsequent 'fanout' calls of next() return copies of the same
 * record. Multiple RecordSinks can consume a single instance of  
 * this Pipe but the caller needs to guarantee that each Sink is 
 * called for every record of the incoming source.  As an aid in
 * keeping the streams in sync (e.g. LockStepDiff), we add a recno
 * field.
 * 
 */
public class BroadcastPipe extends RecordPipe {
    public static final String RECNO_FIELD = "recordno";
	private final int fanout;
	private int instanceNo = 0;
	private Record copy;
	private boolean addRecnoField;
	private long recno = 0;
	
	   public BroadcastPipe(int fanout, boolean addRecNoField) throws IOException {
	       this(null, fanout, addRecNoField);
	   }
	/**
	 * 
	 * @param fanout - the number of consuming RecordSinks (must be greater than 1)
	 * @throws IOException 
	 */
    public BroadcastPipe(RecordPipe root, int fanout, boolean addRecNoField) throws IOException {
        super(root);
		assert(fanout > 1);
		this.fanout = fanout;
	}
	
    @Override
	public Object replicate() throws IOException {
		return new BroadcastPipe(this, fanout, addRecnoField);
	}
	
	@Override
	public Record next() throws IOException {

		// the first instance of the incoming record
		if (instanceNo == 0) {
			Record record = super.next();
			if (record == null) {
			    copy = null;
			    return null;
			}
			
			if (addRecnoField) {
			    record.addField(RECNO_FIELD, recno);
			}
			
			copy = record.getCopy();

			instanceNo++;
			recno++;
			return record;
		}
		
		// the last instance of the incoming record
		else if (instanceNo == fanout - 1) {
			instanceNo = 0; // reset to advance to next record
			return copy;
		}
		
		// all other instances
		else {
			Record rec = copy.getCopy();
			instanceNo++;
			return rec;
		}
	}
}
