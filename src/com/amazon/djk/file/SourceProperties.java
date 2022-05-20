package com.amazon.djk.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.amazon.djk.record.Record;
import com.amazon.djk.record.ThreadDefs;

/**
 * A class for returning all the information about a SOURCE URI
 * and allow for delayed instantiation of the actual SOURCEs.
 */
public class SourceProperties {
	public final static String SOURCE_PROP_TOTAL_RECS = "totalRecs";
	public final static String SOURCE_PROP_SOURCE_FORMAT = "sourceFormat";
    public final static String SOURCE_PROP_KEY_FIELDS = "keyFields";
    public final static String SOURCE_PROP_FIELD_LIST = "sourceFields";
    public final static String SOURCE_PROP_FORMAT_REGEX = "validRegex";
	
	private long totalBytes;
	private long totalRecs;
	private String sourceFormat;
    private final String[] fieldList;
    private final String[] keyFields;
    private final Map<String,String> extras = new HashMap<>();
	
	private final int numFiles;
	private final String sourceURI;
	private final Pattern formatRegex;
	
	private final FormatArgs accessArgs;
	
	private final Record originReport;

	/**
	 * constructor for dir based sources with source.properties
	 * 
	 * @param accessArgs
	 * @param props
	 * @throws IOException
	 */
    public SourceProperties(FormatArgs accessArgs, Properties props, Record originReport) throws IOException {
        this.accessArgs = accessArgs;
        sourceFormat = (String)props.remove(SOURCE_PROP_SOURCE_FORMAT);

        String temp = (String)props.remove(SOURCE_PROP_TOTAL_RECS);
        totalRecs = temp != null ? Long.parseLong(temp) : -1;
        
        temp = (String)props.remove(SOURCE_PROP_FIELD_LIST);
        fieldList = temp != null ? temp.split(",") : new String[0];
        
        temp = (String)props.remove(SOURCE_PROP_KEY_FIELDS);
        keyFields = temp != null ? temp.split(",") : new String[0];
        
        temp = (String)props.remove(SOURCE_PROP_FORMAT_REGEX);
        formatRegex = temp != null ? Pattern.compile(temp) : null;
        
        numFiles = accessArgs.getNumStreams(formatRegex);
        sourceURI = accessArgs.getURI();
        
        for (Object key : props.keySet()) {
            extras.put((String)key, (String)props.get(key));
        }
        
        this.originReport = originReport;  
    }
    
    /**
     * constructor when no properties file exists (e.g. single file, no dir)
     * 
     * @param accessArgs
     * @param format
     * @throws IOException
     */
    public SourceProperties(FormatArgs accessArgs, String format) throws IOException {
        this.accessArgs = accessArgs;
        sourceFormat = format;
        totalRecs = 0;
        fieldList = new String[0];
        keyFields = new String[0];
        
        numFiles = 1;
        sourceURI = accessArgs.getURI();
        originReport = null;
        formatRegex = null;
    }

    public Pattern getValidRegex() {
        return formatRegex;
    }

    public FormatArgs getAccessArgs() {
        return accessArgs;
    }
    
    public int getNumFiles() {
        return numFiles;
    }

    public String getSourceURI() {
        return sourceURI;
    }
    
    public Record getOriginReport() {
    	return originReport;
    }

    public static void write(File dir, long totalRecs, String format, String streamFileRegex, List<String> keyFields) throws IOException {
        write(dir, totalRecs, format, streamFileRegex, keyFields, null);
    }
    
    public static void write(File dir, long totalRecs, String format, String formatRegex, List<String> keyFields, Map<String,String> extras) throws IOException {
		File file = new File(dir, FormatArgs.SOURCE_PROP_FILE);
		FileOutputStream fos = new FileOutputStream(file);
		Properties props = new Properties();

		List<String> fields = ThreadDefs.get().getFieldList();
		props.setProperty(SOURCE_PROP_TOTAL_RECS, Long.toString(totalRecs));
        props.setProperty(SOURCE_PROP_SOURCE_FORMAT, format);
        props.setProperty(SOURCE_PROP_FORMAT_REGEX, formatRegex);
		
        props.setProperty(SOURCE_PROP_FIELD_LIST, StringUtils.join(fields, ","));

        if (keyFields != null && keyFields.size() != 0) {
            props.setProperty(SOURCE_PROP_KEY_FIELDS, StringUtils.join(keyFields, ","));
        }

        if (extras != null) {
            for (Map.Entry<String, String> e : extras.entrySet()) {
                props.setProperty(e.getKey(), e.getValue());
            }
        }
        
        props.store(fos, null);
		fos.close();
	}

    /**
     * get extra parameter by name
     * @param name
     * @return
     */
    public String getExtra(String name) {
        return extras.get(name);
    }
    
	/**
	 * 
	 * @return the total number of bytes if it can be assertained from the 
	 * SOURCE_PROPERTIES_SOURCE otherwise return -1;
	 */
	public long totalBytes() {
		return totalBytes;
	}

	/**
	 * 
	 * @return the total number of records if it can be assertained from the 
	 * SOURCE_PROPERTIES_FILE otherwise return 0;
	 */
	public long totalRecs() {
		return totalRecs;
	}

	/**
	 * 
	 * @return the definitive source format if it can be assertained from the 
	 * SOURCE_PROPERTIES_FILE otherwise return null;
	 */
	public String getSourceFormat() {
		return sourceFormat;
	}
	
	/**
	 * 
	 * @return the definitive source format if it can be assertained from the 
	 * SOURCE_PROPERTIES_FILE otherwise return null;
	 */
	public String[] getSourceFields() {
		return fieldList;
	}
	
	
	public String[] getKeyFields() {
	    return keyFields;
	}
}
