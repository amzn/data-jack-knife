package com.amazon.djk.record;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.amazon.djk.processor.FieldDefs;

/**
 * Non-thread safe field matcher 
 *
 */
public class FieldMatcher {
	protected final boolean[] isFidSought;
	
	/**
	 * 
	 * @param fieldIds
	 * @return
	 */
	public static FieldMatcher create(Set<Short> fieldIds) {
		switch (fieldIds.size()) {
		case 0:
			return new AllFieldMatcher();

		case 1:
			Iterator<Short> it = fieldIds.iterator();
			short oneId = it.next();
			return oneId == FieldDefs.LOCAL_FIELD_TYPE_ID ? new UnmatchableFieldMatcher() : 
				new OneFieldMatcher(oneId);
			
		default:
			return new FieldMatcher(fieldIds);	
		}
	}
	
	public static class UnmatchableFieldMatcher extends FieldMatcher {
	    private UnmatchableFieldMatcher() {
	        super(Collections.emptySet());
	    }
	    
	    /**
	     * even tho we override this, we don't want to call this through
	     * iteration of the entire record, that would be slow.
	     */
	    @Override
	    public boolean isMatch(short id) {
	        return false;
	    }
	}
	
	/**
	 * 
	 */
	public static class OneFieldMatcher extends FieldMatcher {
		private final short oneId;
		
        private OneFieldMatcher(short id) {
            super(Collections.emptySet());
			oneId = id;
		}
		
        @Override
        public boolean isMatch(short id) {
            return id == oneId;
        }
        
        public short getFid() {
            return oneId;
        }
	}

	/**
	 * matches all UNDELETED fields
	 */
    public static class AllFieldMatcher extends FieldMatcher {
		private AllFieldMatcher() {
			super(Collections.emptySet());
		}
		
        @Override
        public boolean isMatch(short id) {
            return true;
        }
	}
	
    /**
     * matches a field of given name only once. 
     */
    public static class OnceMatcher extends FieldMatcher {
        private final boolean[] isFidSoughtCopy;

        public OnceMatcher() throws IOException {
            this(Collections.emptySet());
        }
        
        public OnceMatcher(Set<Short> fieldIds) throws IOException {
            super(fieldIds.size() == 0 ? getAllIds() : fieldIds);
            this.isFidSoughtCopy = new boolean[isFidSought.length];
            for (int i = 0; i < isFidSought.length; i++) {
                isFidSoughtCopy[i] = isFidSought[i];
            }
        }
        
        private static Set<Short> getAllIds() throws IOException {
            List<String> allFields = ThreadDefs.get().getFieldList();
            Set<Short> allIds = new HashSet<>();
            for (String field : allFields) {
                allIds.add(ThreadDefs.get().getId(field));
            }
            return allIds;
        }
        
        public void reset() {
            for (int i = 0; i < isFidSought.length; i++) {
                isFidSought[i] = isFidSoughtCopy[i];
            }            
        }
        
        @Override
        public boolean isMatch(short id) {
            boolean value = super.isMatch(id);
            if (value) {
                isFidSought[id] = false;
            }
            
            return value;
        }
    }
	

	/**
	 * 
	 * @param fieldIds
	 */
	private FieldMatcher(Set<Short> fieldIds) {
		short max = -1;
		for (Short s : fieldIds) {
			max = (short) Math.max(max, s);
		}
		
		isFidSought = new boolean[max+1];
		for (int i = 0; i <= max; i++) {
			isFidSought[i] = false;
		}
		
		for (Short s : fieldIds) {
			isFidSought[s] = true;
		}
	}
	
	/**
	 * 
	 * @param id
	 * @return
	 */
	public boolean isMatch(short id) {
		if (id >= isFidSought.length) return false;
		return isFidSought[id];
	}
}
