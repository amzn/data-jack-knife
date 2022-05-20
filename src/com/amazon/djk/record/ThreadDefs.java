package com.amazon.djk.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.amazon.djk.format.FormatException;
import com.amazon.djk.processor.CoreDefs;
import com.amazon.djk.processor.DJKInitializationException;
import com.amazon.djk.processor.DJKInitializationException.Type;
import com.amazon.djk.processor.KnifeProperties.Namespace;
import com.amazon.djk.record.RecordIO.IOBytes;

/**
 * This non-threadsafe class is instantiated once per-thread via a 
 * ThreadLocal from FieldsFactory.
 *
 */
public class ThreadDefs {
	private static final ThreadLocal<ThreadDefs> defs = ThreadLocal.withInitial(() -> new UnsetThreadDefs());
	
	public static final short NO_SUCH_FIELD = -1;
	private static final int MAX_LEVEL = 5;
	private final Map<String,Short> nameToIdShadow = new HashMap<>(1024);
    private final Map<String,Field> cachedNamedIters = new HashMap<>();
    private final Map<String,OnceEachIterator> cachedOnceEachIters = new HashMap<>();
    private final Map<String,Set<Short>> cachedFieldIdSets = new HashMap<>(); 
	private final List<String> idToNameShadow = new ArrayList<>();
    // for 'local' fields
    private final Map<String,Object> localNameToValue = new HashMap<>(1024);

	private final Inflater inflater = new Inflater();
	private final Deflater deflater = new Deflater();
	private final Bytes cachedBytes = new Bytes();

	// below for recursive contexts
	private final StringBuilder stringBuilder = new StringBuilder();
	private final FieldIterator fieldIterator = new FieldIterator();
	private static final int MIN_COMPRESSABLE_NUM_BYTES = 32;
	
	private final Bytes utf8Bytes = new Bytes();
    private final UTF8BytesRef utf8BytesRef = new UTF8BytesRef();
	
    private final CoreDefs coreDefs;
    
    public static class UnsetThreadDefs extends ThreadDefs {
    	public UnsetThreadDefs() {
			super(null);
		}
    }
    
    public static ThreadDefs get() throws IOException {
    	ThreadDefs tds = defs.get();
    	if (tds instanceof UnsetThreadDefs) {
    		throw new DJKInitializationException(Type.THREAD_DEFS);
    	}
    	
    	return tds;
    }
    
    public static boolean isInitialized() {
    	ThreadDefs tds = defs.get();
    	return (! (tds instanceof UnsetThreadDefs) );
    }
    
	public static void initialize(CoreDefs cdefs) {
		defs.set(new ThreadDefs(cdefs));
	}
	
	public static void deinitialize() {
		defs.set(new UnsetThreadDefs());
	}
    
    private ThreadDefs(CoreDefs coreDefs) {
    	this.coreDefs = coreDefs;
    }

	public UTF8BytesRef getUTF8BytesRef(final CharSequence s) {
        utf8Bytes.reset();
        UnicodeUtils.putUTF8Bytes(s, utf8Bytes);
        utf8BytesRef.set(utf8Bytes);
        return utf8BytesRef;
    }
	
	/**
	 * 
	 * @param bytes
	 * @return
	 */
	public String getUTF8BytesRefAsString(UTF8BytesRef bytes) throws FormatException {
		try {
			UnicodeUtils.getUTF8BytesRefAsString(bytes, stringBuilder);
			return stringBuilder.toString();
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new FormatException(("UTF8 encoding error"));
		}
	}

	/**
	 * for convenience 
	 * 
	 * @param value
	 * @param out
	 */
	public void putUTF8Bytes(String value, Bytes out) {
	    UnicodeUtils.putUTF8Bytes(value, out);
	}
	
	/**
	 * 
	 * @param name the name of the field
	 * @return the associated field id or -1 if non-existent
	 */
	public short getId(String name) {
    	Short fid = nameToIdShadow.get(name);
    	if (fid != null) return fid;
    		
    	fid = coreDefs.getId(name);
    	if (fid == null) return NO_SUCH_FIELD;
    	
    	nameToIdShadow.put(name, fid);
    	return fid;
	}
	
	/**
	 * 
	 * @param id
	 * @return
	 */
	public String getName(short id) {
		if (id < idToNameShadow.size()) {
			return idToNameShadow.get(id);
		}

		String name = coreDefs.getName(id);
		if (name == null) return null;
		
		id = (short)(idToNameShadow.size()); // first missing id
		while (true) {
    		String gName = coreDefs.getName(id++);
    		if (gName == null) break;
    		idToNameShadow.add(gName);
    	}

		return name;
	}
	
    /**
     * 
     * @param name name of the field for which a fieldId is to be returned.
     *
     * @return
     * @throws IllegalFieldException 
     */
    public short getOrCreateFieldId(String name) throws IllegalFieldException {
        Short id = nameToIdShadow.get(name);
        if (id != null) return id;

        id = coreDefs.getOrCreateGlobalId(name);
        nameToIdShadow.put(name, id);

        return id;
    }
    
    /**
     * 
     * @param fieldName name of the field for which a fieldId is to be returned.
     * @return
     * @throws IllegalFieldException 
     */
    public short getFieldIdOrSetLocal(String fieldName, Object value) throws IllegalFieldException {
        Short id = nameToIdShadow.get(fieldName);
        if (id == null) {
            id = coreDefs.getOrCreateGlobalId(fieldName);
            nameToIdShadow.put(fieldName, id);
        }
        
        if (id != -1) return id;

        // we are a local field
        
        localNameToValue.put(fieldName, value);

        return id;
    }
    
    private static class NullValue { };
    
    /**
     * 
     * @param fieldName
     * @return null if no local value or the value
     */
    public Object getLocalValue(String fieldName) {
        Object o = localNameToValue.get(fieldName);
        // o=null-->never seen before
        // o=NullValue --> value is known to be undefined
        if (o instanceof NullValue) return null;
        if (o != null) return o;

        // we've never seen before
        o = coreDefs.getLocalFieldInitialValue(fieldName);
        localNameToValue.put(fieldName, o == null ? new NullValue() : o); // store it
        return o; // either defined or null
    }
    
    /*
     * TODO: make private or package
     */
    public void testOnlyPrintFieldDefs() {
        System.out.println("--- Thread Local Field Defs:");
        System.out.println("\nidToNameShadow");
        for (int i = 0; i < idToNameShadow.size(); i++) {
            System.out.println(String.format("%3d -> %-20.20s", i, idToNameShadow.get(i)));
        }
        
        System.out.println("\nnameToIdShadow");
        for (Map.Entry<String, Short> e : nameToIdShadow.entrySet()) {
            System.out.println(String.format("%-20.20s -> %3d", e.getKey(), e.getValue()));
        }
        
        System.out.println("\nlocalNameToValue");
        for (Map.Entry<String, Object> e : localNameToValue.entrySet()) {
            System.out.println(String.format("%-20.20s -> %s", e.getKey(), e.getValue().toString()));
        }
        
        coreDefs.testOnlyPrintFieldDefs();
    }
    
	public Class<?> getFieldType(String name) {
		return coreDefs.getFieldType(name);
	}
    
	/**
	 * 
	 * @param fieldSpecs field names possibly ending/starting with '+' denoting
	 * a field prefix/suffix.
	 * @param fields will contain all currently defined fields matching the fieldSpecs 
	 */
	public void getFieldMatches(String[] fieldSpecs, List<String> fields) {
		fields.clear();
		getFieldList();
		for (String spec : fieldSpecs) {
			if (spec.endsWith("+")) {
                spec = spec.substring(0, spec.length()-1);
                for (int i = 0; i < idToNameShadow.size(); i++) {
                    String field = idToNameShadow.get(i);
                    if (field.startsWith(spec)) {
                        fields.add(field);
                    }
                }
			}
			
			else if (spec.startsWith("+")) {
                spec = spec.substring(1);
                for (int i = 0; i < idToNameShadow.size(); i++) {
                    String field = idToNameShadow.get(i);
                    if (field.endsWith(spec)) {
                        fields.add(field);
                    }
                }
			}
			
			else {
				fields.add(spec); // plain field
			}
		}
	}

	/**
	 * WARNING: do not use in recursive contexts
	 * 
	 * @param name
	 * @return
	 * @throws IOException 
	 */
	Field getCachedField(String name) throws IOException {
		if (name == null) return null;
        Field field = cachedNamedIters.get(name);
		if (field == null) {
			field = new Field(name);
			cachedNamedIters.put(name, field);
		}
		
		return field;
	}

	/**
	 *
	 * @return
	 */
	FieldIterator getFieldIterator() {
		return fieldIterator;
	}

	/**
	 *
	 * @return
	 */
	StringBuilder getStringBuilder() {
		return stringBuilder;
	}


	/**
	 * WARNING: do not use in recursive contexts
	 *
	 * @return a reset() BytesRef for this thread
	 */
	Bytes getCachedBytes() {
		cachedBytes.reset();
		return cachedBytes;
	}

	/**
	 * 
	 * @return
	 */
	public List<String> getFieldList() {
		short i = 0;
		// loop thu to assure shadow up to date
		while (true) {
			String temp = getName(i++);
			if (temp == null) break;
		}
		
		return idToNameShadow;
	}
	
	/**
	 * specialized deflate uses first byte of output to encode whether deflation
	 * has occured.  In this way, it can avoid compressing small data.  Must use
	 * with cooresponding 'inflate' method.
	 *  
	 * @param in
	 * @param out
	 * @throws IOException
	 */
	public void deflate(Bytes in, Bytes out) throws IOException {
	    out.reset();
	    
	    out.putVarLenUnsignedInt(in.size());
	    if (in.size() < MIN_COMPRESSABLE_NUM_BYTES) { // don't compress
	        out.putBytes(in);
	        return;
	    }
	    
	    deflater.reset();
        deflater.setInput(in.bytes, 0, in.length);
        deflater.finish();
        
        try {
            while (!deflater.finished()) {
                out.resize(in.length + 1);
                int count = deflater.deflate(out.bytes, out.length, out.capacity() - out.length);
                out.length += count;
            }
        }
        
        catch (Exception e) {
            throw new IOException("in.size=" + in.size() + " out.capacity=" + out.capacity(), e);
        }
	}

	/**
	 * 
	 * @param compressedBytes
	 * @param out
	 * @throws IOException
	 */
    public void inflate(Bytes compressedBytes, Bytes out) throws IOException {
        int uncompressedSize = compressedBytes.getVarLenUnsignedIntAt(0);
        int offset = compressedBytes.getLastNumVarLenBytes();

        if (uncompressedSize < MIN_COMPRESSABLE_NUM_BYTES) {
            out.putBytes(compressedBytes.bytes, offset, compressedBytes.size() - offset);
            return;
        }
        
        inflater.reset();
        inflater.setInput(compressedBytes.bytes, offset, compressedBytes.size() - offset);
        
        out.resize(uncompressedSize);
        int todo = uncompressedSize;
        try {
            while (!inflater.finished()) {
                int count = inflater.inflate(out.bytes, out.length, todo);
                out.length += count;
                todo -= count;
                
                if (count == 0) break;
            }
        }
           
        catch (DataFormatException e) {
            throw new IOException(e);
        }
    }

    /**
     * 
     * @param compressedBytes
     * @param out
     * @param compressedBytesWithSize if true, the input bytes contain a prepended varInt of uncompressed size
     * @throws IOException 
     */
    public void inflate(IOBytes compressedBytes, IOBytes out, boolean compressedBytesWithSize) throws IOException {
        if (compressedBytesWithSize) {
            inflate(compressedBytes, out);
            return;
        }
        
        inflater.reset();
        inflater.setInput(compressedBytes.bytes, 0, compressedBytes.size());

        try {
            while (!inflater.finished()) {
                out.resize(32 * 1024);                 
                int count = inflater.inflate(out.bytes, out.length, out.capacity()-out.length);
                out.length += count;
                
                if (count == 0) break;
            }
        }
           
        catch (DataFormatException e) {
            throw new IOException(e);
        }
    }

	public int getNumSourceThreads() {
		return coreDefs.getNumSourceThreads();
	}
	
	public int getNumSinkThreads() {
		return coreDefs.getNumSinkThreads();
	}

	public Namespace getPropertiesNamespace() {
		return coreDefs.getPropertiesNamespace();
	}

	public int getDoublePrintPrecision() {
		return coreDefs.getDoublePrintPrecision();
	}

	public int getNumSortBuckets() {
		return coreDefs.getNumSortBuckets();
	}

	public int getNumSortThreads() {
		return coreDefs.getNumSortThreads();
	}
}
