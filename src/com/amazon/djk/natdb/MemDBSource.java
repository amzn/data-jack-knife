package com.amazon.djk.natdb;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.amazon.djk.core.Splittable;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.processor.InnerKnife;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.ThreadDefs;

public class MemDBSource extends MemDBKeyedSource implements Splittable {

    public MemDBSource(OpArgs args) throws IOException {
        super(args);
	}

	/**
	 * for split
	 * 
	 * @param sourceUri
	 * @param keys
	 * @param map
	 * @throws IOException
	 */
	protected MemDBSource(OpArgs args,
			ConcurrentHashMap<Record, MapRecord> map,
			BlockingQueue<Record> nextkeys) throws IOException {
		super(args, map, nextkeys);
	}

	@Override
	public void finishSinking(InnerKnife processor) {
		Set<Record> keys = map.keySet();
		Iterator<Record> keysIter = keys.iterator();
		while (keysIter.hasNext()) {
			Record key = keysIter.next();
			this.nextKeys.add(key);
		}
		numUniqRecs = keys.size();
        nextKeys.add(poisonRecord);
	}
	
	@Override
	public Object split() throws IOException {
		if (numInstances >= ThreadDefs.get().getNumSinkThreads()) {
			return null;
		}

		numInstances++;
		return new MemDBSource(args, map, nextKeys);
	}
}
