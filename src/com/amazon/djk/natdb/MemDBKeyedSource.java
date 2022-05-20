package com.amazon.djk.natdb;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.keyed.KeyedSource;
import com.amazon.djk.keyed.OuterKeyedSource;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.processor.InnerKnife;
import com.amazon.djk.processor.NeedsSource;
import com.amazon.djk.processor.WithInnerSink;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.KeyMaker;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;

@ReportFormats2(lineFormats = { "numUniqKeys=%,d", })
public class MemDBKeyedSource extends OuterKeyedSource implements WithInnerSink, NeedsSource {
    public static final String DEFAULT_SUBNAME = "child";
    public static final String SUBNAME_PARAM = "child";
    public static final String NO_GROUP = "NONE";

    public enum MapType {ONE_PER_KEY, GROUP_BY}; // i.e. keeps the last instance of record with key
	protected final ConcurrentHashMap<Record, MapRecord> map;
	protected final BlockingQueue<Record> nextKeys;
	private final MemDBKeyedSink mapSink;
	protected final PoisonRecord poisonRecord = new PoisonRecord();

	protected int numInstances = 1;
	private class PoisonRecord extends Record {};

	private final KeyMaker keyMaker;
	private final Record keyRecord = new Record();
	
	@ScalarProgress(name = "numUniqKeys")
	protected long numUniqRecs;

    public MemDBKeyedSource(OpArgs args) throws IOException {
        super(args, (Fields)args.getArg(KEYS));
        
        Field subName = (Field)args.getParam(SUBNAME_PARAM);
        boolean onePerKey = subName.getName().equals(NO_GROUP);
        
		mapSink = onePerKey ?
		        new MemDBKeyedSink(args) :
		        new MemDBGroupSink(args);

		map = mapSink.getMap();
		nextKeys = new LinkedBlockingQueue<>();
		keyMaker = new KeyMaker(getKeyFields());
	}

	/**
	 * for replicateKeyed()
	 * 
	 * @param sourceUri
	 * @param keys
	 * @param map
	 * @throws IOException
	 */
	protected MemDBKeyedSource(OpArgs args,
			ConcurrentHashMap<Record, MapRecord> map,
			BlockingQueue<Record> nextKeys) throws IOException {
		super(args, (Fields)args.getArg(KEYS));
		this.nextKeys = nextKeys;
		this.map = map;
		mapSink = null;
		keyMaker = new KeyMaker(getKeyFields());
	}

	@Override
	public void finishSinking(InnerKnife processor) {
        Set<Record> keys = map.keySet();
		// don't move keys to nextKeys queue here 
		// only reason to next in Keyed context is right join
		// for unaccessed keys
		numUniqRecs = keys.size();
	}
	
	@Override
	public long getNumRecords() {
		return numUniqRecs;
	}
	
	@Override
	public Object replicateKeyed() throws IOException {
		return new MemDBKeyedSource(args, map, nextKeys);
	}

	@Override
	public void enableOuterAccess() {
		// always enabled
	}

	@Override
	public void prepareOuterAccess() {
	    Set<Record> keys = map.keySet();
        Iterator<Record> keysIter = keys.iterator();
        while (keysIter.hasNext()) {
            Record key = keysIter.next();
            MapRecord valueRecord = map.get(key);
            if (!valueRecord.getWasAccessed()) {
                nextKeys.add(key);
            }
        }
        
        nextKeys.add(poisonRecord);
	}
	
	@Override
	public Record next() throws IOException {
	    Record keyRecord = null;
	    
        try {
            keyRecord = nextKeys.take();
        } catch (InterruptedException e) {
            return null;
        }

        if (keyRecord instanceof PoisonRecord) {
            nextKeys.add(keyRecord); // put back
	        return null;
	    }
	    
	    MapRecord valueRecord = map.remove(keyRecord);
	    keyRecord.addFields(valueRecord);
	    
	    return keyRecord;
	}

	@Override
    public Record getValue(Record lookupRecord) throws IOException {
		keyRecord.reset();
		keyMaker.copyTo(lookupRecord, keyRecord);
		MapRecord valrec = map.get(keyRecord);
		if (valrec == null) return null;
		valrec.setWasAccessed(); // in case of right join
		return valrec;
	}
	
	@Override
    public Record getValueOnce(Record lookupRecord) throws IOException {
		keyRecord.reset();
		keyMaker.copyTo(lookupRecord, keyRecord);
		MapRecord valrec = map.get(keyRecord);
		if (valrec == null || valrec.getWasAccessed()) return null;
		valrec.setWasAccessed(); // in case of right join
		return valrec;
	}

	@Override
	public RecordSink getSink() {
		return mapSink;
	}
	
	@Override
	public void addSource(RecordSource source) {
		mapSink.addSource(source);
	}
	
    @Description(text = { "Provides the ability to map a single record to a KEY. See also groupDB and mapDB."},
    contexts = { "INPUT map:KEYS" })
	@Gloss(entry = "INPUT", def = "expression describing the input to the map.")
    @Arg(name = "KEYS", gloss = "comma separated list of key fields.", type = ArgType.FIELDS)
    @Example(expr="[ id:1,color:blue id:2,color:blue id:3,color:red ] map:color", type=ExampleType.EXECUTABLE)
	public static class MapOp extends SourceOperator {
        private final boolean isMap;
        
		public MapOp() {
            super("map:KEYS", Type.USAGE);
            isMap = true;
        }
		
		public MapOp(String usage) {
		    super(usage, Type.USAGE);
		    isMap = false;
		}

        @Override
        public RecordSource getSource(OpArgs args) throws IOException, SyntaxError {
            if (isMap) {
                args.addAnnotationLessParam(DEFAULT_SUBNAME, new Field(NO_GROUP));
            }
            return new MemDBSource(args);
        }
        
        @Override
        public KeyedSource getKeyedSource(OpArgs args) throws IOException, SyntaxError {
            if (isMap) {
                args.addAnnotationLessParam(DEFAULT_SUBNAME, new Field(NO_GROUP));
            }
            return new MemDBKeyedSource(args);
        }
	}

    @Description(text = { "Provides 'grouping' functionality depending on the 'child' parameter.  For each record of the input",
    "the non-KEY fields are stored in map by the KEY fields under a subrecord named 'child'.  See also groupDB and mapDB and uniq."},
    contexts = { "INPUT group:KEYS" })
    @Param(name = SUBNAME_PARAM, gloss = "name of the subrecord field within a group record.  However, if child=NONE, only the first record instance is mapped to the KEY using a flat structure. (i.e. no subrecord).", type = ArgType.FIELD, eg="NONE", defaultValue = DEFAULT_SUBNAME)
    @Example(expr="[ id:1,color:blue id:2,color:blue id:3,color:red ] group:color", type=ExampleType.EXECUTABLE) 
	public static class GroupOp extends MapOp {
	    public GroupOp() {
	        super("group:KEYS");
	    }
	}
}
