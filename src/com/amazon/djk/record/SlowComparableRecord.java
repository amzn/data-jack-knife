package com.amazon.djk.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * primarily for testing purposes
 * 
 * cononicalizes the order of the fields and removes deletes so that equals and hashCode are valid.
 *
 */
public class SlowComparableRecord extends Record {
	public final static String LEFT_CHILD = "LEFT";
	public final static String RIGHT_CHILD = "RIGHT";
    private FieldIterator fiter = new FieldIterator();
    private Map<Short,List<Bytes>> fields = new HashMap<>();
    private Map<Short,List<Bytes>> subs = new HashMap<>();
    
    public static Record create(Record rec) throws IOException {
        SlowComparableRecord cr = new SlowComparableRecord(rec);
        return cr.get();
    }
    
    protected void putShort(short value) {
        super.putShort(value);
    }
    
    protected void putBytes(BytesRef ref) {
        super.putBytes(ref);
    }
    
    private SlowComparableRecord(Record record) throws IOException {
        fiter.init(record);
        
        while (fiter.next()) {

            short fid = fiter.getId();
            
            if (fiter.getType() == FieldType.RECORD) {
                Record temp = new Record();
                fiter.getValueAsRecord(temp);
                
                List<Bytes> slist = subs.get(fid);
                if (slist == null) {
                    slist = new ArrayList<>();
                    subs.put(fid, slist);
                }
                
                SlowComparableRecord crec = new SlowComparableRecord(temp);
                Record foo = crec.get();
                slist.add(foo);
            }
            
            else {
                Bytes fieldBytes = fiter.getValueAsFieldBytes();
                List<Bytes> blist = fields.get(fid);
                if (blist == null) {
                    blist = new ArrayList<>();
                    fields.put(fid, blist);
                }
                
                blist.add(fieldBytes);
            }
        }
        
        // sort the field lists and sub lists by the value
        for (Short fid : fields.keySet()) {
            List<Bytes> bytesList = fields.get(fid);
            Collections.sort(bytesList);
        }

        for (Short sid : subs.keySet()) {
        	List<Bytes> subRecs = subs.get(sid);
            Collections.sort(subRecs);
        }
    }
    
    /**
     * gets a record that can be compared using equals.
     * 
     * @return
     * @throws IOException
     */
    private Record get() throws IOException {
        Short[] fids = fields.keySet().toArray(new Short[0]);
        Arrays.sort(fids);
        for (Short fid : fids) {
            List<Bytes> bytesList = fields.get(fid);
            
            // creates the actual extended record
            for (Bytes bytes : bytesList) {
                putShort(fid);
                putBytes(bytes);
            }
        }
        
        if (subs.size() == 0) return this;
        
        // do sub records
        Short[] sids = subs.keySet().toArray(new Short[0]);
        Arrays.sort(sids);

        for (Short sid : sids) {
            List<Bytes> subRecs = subs.get(sid);
            String field = ThreadDefs.get().getName(sid);
            for (Bytes r : subRecs) {
                addField(field, (Record)r);
            }
        }
        
        return this;
    }
    
    
    /**
     * slow slow method to compare two lists of records for equality, where order in the records does not matter.
     * 
     * @param a
     * @param b
     * @return
     * @throws IOException
     */
    public static boolean areEqual(List<Record> a, List<Record> b) throws IOException {
        if (a.size() != b.size()) return false;
        
        for (int i = 0; i < a.size(); i++) {
            a.set(i, SlowComparableRecord.create(a.get(i)));
        }
        
        for (int i = 0; i < b.size(); i++) {
            b.set(i, SlowComparableRecord.create(b.get(i)));
        }

        Collections.sort(a);
        Collections.sort(b);
        
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).equals(b.get(i))) {
            	System.out.println();
                System.out.println(a.get(i));
                System.out.println(" is not equal to ");
            	System.out.println();
                System.out.println(b.get(i));
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Slow method to diff records
     * 
     * @param left
     * @param right
     * @return a difference record with LEFT and RIGHT sub records or null if identical
     * @throws IOException
     */
	public static Record diff(Record left, Record right) throws IOException {
		Record out = new Record();
		return diff(out, left, right) ? out : null;
	}
    
    /**
     *
     * Slow slow method to diff 2 records and produce a record with 2 possible subrecords: LEFT, RIGHT
     * to contain field-values pairs that only occur in those respective records.  Subrecords are NOT 
     * diffed recursively and fields with duplicate names are handled as set (either equal or not)
     * @param out output record.
     * @param left
     * @param right
     * returns true if there was a difference in left and right, else false;
     * 
     * @throws IOException
     */
	public static boolean diff(Record out, Record left, Record right) throws IOException {
    	SlowComparableRecord lf = new SlowComparableRecord(left);
        SlowComparableRecord rt = new SlowComparableRecord(right);
        
        Record leftDiff = new Record();
        Record rightDiff = new Record();
        
        doDiff(false, lf.fields, rt.fields, leftDiff, rightDiff);
        doDiff(true, lf.subs, rt.subs, leftDiff, rightDiff);
        
        if (leftDiff.size() == 0 && rightDiff.size() == 0) {
        	return false;
        }
	
        if (leftDiff.size() > 0) {
        	out.addField(LEFT_CHILD, leftDiff);
        }
        
        if (rightDiff.size() > 0) {
        	out.addField(RIGHT_CHILD, rightDiff);
        }
        
        return true;
    }

    /**
     * slow slow method to compare two records
     * 
     * @param r1
     * @param r2
     * @throws IOException 
     */
    public static boolean areEqual(Record r1, Record r2) throws IOException {
        Record a = SlowComparableRecord.create(r1);
        Record b = SlowComparableRecord.create(r2);
        return a.equals(b);
    }

	public static boolean areEqual(Queue<Record> answer, Queue<Record> recs) throws IOException {
		Record[] a = answer.toArray(new Record[0]);
		Record[] b = recs.toArray(new Record[0]);
		return areEqual(Arrays.asList(a), Arrays.asList(b));
	}
	
	/**
	 * helper function to diff Field Bytes and Sub Records
	 * @param isRecord
	 * @param left
	 * @param right
	 * @param leftDiff
	 * @param rightDiff
	 * @throws IOException
	 */
	private static void doDiff(boolean isRecord, Map<Short, List<Bytes>> left,
			Map<Short, List<Bytes>> right, Record leftDiff, Record rightDiff)
			throws IOException {
		Set<Short> fids = new HashSet<>();
		fids.addAll(left.keySet());
		fids.addAll(right.keySet());

		for (Short fid : fids) {
			List<Bytes> leftBytesList = left.containsKey(fid) ? (List<Bytes>) left.get(fid) : 
				Collections.emptyList();
			List<Bytes> rightBytesList = right.containsKey(fid) ? (List<Bytes>) right.get(fid) : 
				Collections.emptyList();

			int lf_idx = 0;
			int rt_idx = 0;
			while (lf_idx < leftBytesList.size()
					|| rt_idx < rightBytesList.size()) {
				Bytes lf_bytes = lf_idx < leftBytesList.size() ? leftBytesList.get(lf_idx) : null;
				Bytes rt_bytes = rt_idx < rightBytesList.size() ? rightBytesList.get(rt_idx) : null;

				if (lf_bytes == null && rt_bytes != null) {
					copyTo(isRecord, fid, rt_bytes, rightDiff);
					rt_idx++;
					continue;
				}

				if (lf_bytes != null && rt_bytes == null) {
					copyTo(isRecord, fid, lf_bytes, leftDiff);
					lf_idx++;
					continue;
				}

				int cmp = lf_bytes.compareTo(rt_bytes);
				if (cmp == 0) {
					lf_idx++;
					rt_idx++;
					continue; // skip both!
				}

				if (cmp < 0) { // left < right
					// output left and advance left
					copyTo(isRecord, fid, lf_bytes, leftDiff);
					lf_idx++;
					continue;
				}

				if (cmp > 0) { // left > right
					// output right and advance right
					copyTo(isRecord, fid, rt_bytes, rightDiff);
					rt_idx++;
				}
			}
		}
	}

	/**
	 * 
	 * @param isRecord
	 * @param fid
	 * @param bytes
	 * @param out
	 * @throws IOException
	 */
	private static void copyTo(boolean isRecord, short fid, Bytes bytes, Record out) throws IOException {
		if (isRecord) {
			String field = ThreadDefs.get().getName(fid);
			out.addField(field, (Record)bytes);				
		} else {
			out.putShort(fid);
			out.putBytes(bytes);
		}
	}
}
