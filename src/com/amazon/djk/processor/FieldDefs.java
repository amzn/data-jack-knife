package com.amazon.djk.processor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.record.IllegalFieldException;

/**
 * This class is exxtended by the singleton CoreDefs class
 */
public class FieldDefs {
	private static final Logger logger = LoggerFactory.getLogger(FieldDefs.class);
    private final static Pattern legalFieldNamePattern = Pattern.compile("^[A-Za-z0-9_.]+$");
    private final static Pattern illegalFieldCharsRegex = Pattern.compile("[^A-Za-z0-9_.]");
	public final static String DELETED_FIELD = "_deleted_";
    public final static String INTERNAL_FIELD_NAME = "_internal_";
	public static final short DELETED_FIELD_ID = 0;
    public static final short INTERNAL_FIELD_ID = 1;
    public static final short LOCAL_FIELD_TYPE_ID = -1;
	public static final int MAX_NUM_FIELDS = 0x3FFF;
	
	private final Map<String,Short> gNameToId = new ConcurrentHashMap<>();
	private final List<String> gIdToName = new CopyOnWriteArrayList<>();
    private final Map<String,Object> gEphemeralNameToInitialValue = new ConcurrentHashMap<>(1024);
	// not shadowed in thread local version because no write contention
    private final Map<String,Class<?>> gNameToType = new ConcurrentHashMap<>(1024);
    protected final AtomicReference<FieldNameRules> fieldNameRules = new AtomicReference<FieldNameRules>();

    public enum FieldNameRules {
        ENFORCE_REGEX(0), // field names must adhere to legalFieldNamePattern
        CONVERT_TO_UNDERBAR(1), // illegal characters will be converted to underbar
        ALL_BUT_COLON(2); // all but colon characters will be allowed

        private final int intValue;
        private FieldNameRules(int intValue) {
            this.intValue = intValue;
        }

        public static FieldNameRules getType(String stringValue) {
            for (FieldNameRules fnh : FieldNameRules.values()) {
                if (fnh.toString().equals(stringValue)) {
                    return fnh;
                }
            }

            return ENFORCE_REGEX;
        }
    }

    FieldDefs() {
    	try {
    		// field ids we can always count on being absolute
    		getOrCreateGlobalId(DELETED_FIELD);  // fid = 0       
    		getOrCreateGlobalId(INTERNAL_FIELD_NAME); // fid = 1
    	}
    	
    	catch (IllegalFieldException e) { 
    		// no worries 
    	}
	}

	/**
	 * Returns the primitive type of the field if declared.  Not shadowed
	 * 
	 * @param fieldName name of the field
	 * @return the class of the field (Long, Double, String) or null if not defined.
	 */
    public Class<?> getFieldType(String fieldName) {
        return gNameToType.get(fieldName);
    }
    
    /**
     * declares the type for a field
     * @param fieldName
     * @param typeName
     * @throws SyntaxError
     */
    public synchronized void declareField(String fieldName, String typeName) throws SyntaxError {
        Class<?> clazz = Object.class;
        
        switch (typeName) {
        case "STRING":
            clazz = String.class;
        break;

        case "LONG":
            clazz = Long.class;
            break;
        
        case "DOUBLE":
            clazz = Double.class;
            break;
        
        case "BOOLEAN":
            clazz = Boolean.class;
            break;
            
        default:
            throw new SyntaxError("unknown cast type: " +  typeName);
        }
        
        gNameToType.put(fieldName, clazz);
    }

	/**
	 * 
	 * @param fid
	 * @return
	 */
	public synchronized String getName(short fid) {
		if (fid >= gIdToName.size()) return null;
		return gIdToName.get(fid);
	}
	
	/**
	 * getOrCreateFieldId
	 * @param name
	 * @return the fieldId of field 'name' or -1 if 'name' represents a 'local' field
	 * @throws IllegalFieldException 
	 */
    public synchronized short getOrCreateGlobalId(String name) throws IllegalFieldException {
    	Object initialValue = gEphemeralNameToInitialValue.get(name);
    	if (initialValue != null) {
    		logger.debug(String.format("accessing global local field=%s value=%s", name, initialValue));
    		return LOCAL_FIELD_TYPE_ID;
    	}

    	Matcher m = legalFieldNamePattern.matcher(name);
    	if (!m.matches()) {
    	    switch (fieldNameRules.get()) {
                case ENFORCE_REGEX:
                    throw new IllegalFieldException(String.format("illegal field name='%s'. Must match regex='%s'. Consider using '%s' property.\n" +
                            "unix> export %s={%s or %s or %s}; djk [ hello:world ]\n" +
                            "unix> djk %s=%s\\; [ hello:world ]",
                            name, legalFieldNamePattern.toString(), CoreDefs.FIELD_NAME_RULES, // first line
                            CoreDefs.FIELD_NAME_RULES, // second line
                            FieldNameRules.ENFORCE_REGEX.toString(), // second line
                            FieldNameRules.CONVERT_TO_UNDERBAR.toString(), // second line
                            FieldNameRules.ALL_BUT_COLON.toString(), // second line
                            CoreDefs.FIELD_NAME_RULES, FieldNameRules.ENFORCE_REGEX.toString() // third line
                            ));

                case CONVERT_TO_UNDERBAR:
                    name = illegalFieldCharsRegex.matcher(name).replaceAll("_");
                    break;

                case ALL_BUT_COLON:
                    name = name.replace(':', '_'); // non regex
                    break;
            }
    	}

        Short fid = gNameToId.get(name);
        if (fid != null) return fid;

        if (gIdToName.size() > MAX_NUM_FIELDS) {
            throw new IllegalFieldException("too many fields defined");
        }
    	
		fid = (short)gNameToId.size();
		gIdToName.add(name);

		logger.debug(String.format("defining global field=%s id=%d", name, fid));
		gNameToId.put(name, fid);
		
		return fid;
    }

	public synchronized Short getId(String name) {
		return gNameToId.get(name);
	}

    /**
     * FIXME: do we really need Class<?> here and above in declareFields?
     * 
     * It is illegal to attempt to define a field as local AFTER it has already
     * been defined otherwise.
     * 
     * @param field
     * @param type
     * @param value
     * @throws SyntaxError
     */
    public synchronized void defineEphemeralField(String field, String type, String value) throws SyntaxError {
        Short fid = gNameToId.get(field);
        if (fid != null) {
            throw new SyntaxError(String.format("It is currently illegal to define a local field for which a field id has already been defined.  field=%s", field));
        }
        
        Class<?> clazz = Object.class;
        switch (type) {
        case "STRING":
            clazz = String.class;
            gEphemeralNameToInitialValue.put(field, value);
        break;

        case "LONG":
            clazz = Long.class;
            gEphemeralNameToInitialValue.put(field, Long.parseLong(value));
            break;
        
        case "DOUBLE":
            clazz = Double.class;
            gEphemeralNameToInitialValue.put(field, Double.parseDouble(value));
            break;
        
        case "BOOLEAN":
            clazz = Boolean.class;
            gEphemeralNameToInitialValue.put(field, Boolean.parseBoolean(value));
            break;
            
        default:
            throw new SyntaxError("unknown cast type");
        }
        
        gNameToType.put(field, clazz);
        
    }
    
    public synchronized Object getLocalFieldInitialValue(String name) {
        return gEphemeralNameToInitialValue.get(name);
    }

	public void testOnlyPrintFieldDefs() {
        System.out.println("--- JackKnife Field Defs:");
        System.out.println("\nidToName");
        for (int i = 0; i < gIdToName.size(); i++) {
            System.out.println(String.format("%d -> %-20.20s", i, gIdToName.get(i)));
        }
        
        System.out.println("\nnameToId");
        for (Map.Entry<String, Short> e : gNameToId.entrySet()) {
            System.out.println(String.format("%-20.20s -> %d", e.getKey(), e.getValue()));
        }
        
        System.out.println("\nnameToInitialValue");
        for (Map.Entry<String, Object> e : gEphemeralNameToInitialValue.entrySet()) {
            System.out.println(String.format("%-20.20s -> %s", e.getKey(), e.getValue()));
        }
        
        System.out.println("\nnameToType");
        for (Map.Entry<String, Class<?>> e : gNameToType.entrySet()) {
            System.out.println(String.format("%-20.20s -> %s", e.getKey(), e.getValue()));
        }
    }
}
