package com.amazon.djk.file;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * class to manage multiple FileSystem-s
 * 
 */
public class FileSystems {
	private static Logger logger = LoggerFactory.getLogger(FileSystems.class);
	public final static String DJK_STREAM_BUFFER_KBS_PROP = "DJK_STREAM_BUFFER_KBS";
	private final int streamBufferKBs; 
	private final Map<String, FileSystem> schemeToFactory = new HashMap<>();
    private final FileSystem localFilesys = new LocalFileSystem();

	public FileSystems() {
		streamBufferKBs = Integer.parseInt(System.getProperty(DJK_STREAM_BUFFER_KBS_PROP, "1024"));
		FileSystem[] builtInFactories = {localFilesys, new SystemFileSystem()};
		for (FileSystem factory: builtInFactories) {
			addFileSystem(factory);
		}
	}
	
	public Map<String,FileSystem> getFileSystems() {
	    return schemeToFactory;
	}

	public Collection<FileSystem> getList() {
		return schemeToFactory.values();
	}
	
	public void addFileSystem(FileSystem factory) {
		FileSystem ourFactory = schemeToFactory.get(factory.scheme());
		
		if (ourFactory == localFilesys) { // just ignore it
		    return;
		}
		
		if (ourFactory != null) {
			throw new RuntimeException("Improperly configured jackknife." +  
                    "Stream factory scheme collision for '" + factory.scheme() +
                    "' from '" + factory.getClass().getSimpleName() + "'");			
		}
		
		schemeToFactory.put(factory.scheme(), factory);
	}
	
    /**
     * 
     * @param uri
     * @return
     */
    public FileSystem getFileSystem(String scheme) {
    	FileSystem filesys = schemeToFactory.get(scheme);
    	if (filesys != null) return filesys;
    	return localFilesys;
    }
    
    /**
     * 
     * @throws IOException
     */
	public void close() throws IOException {
		for(FileSystem factory : schemeToFactory.values()) {
			if (factory != null)
				factory.close();
		}
	}

	/**
	 * 
	 * @param scheme
	 * @return
	 */
	public boolean isScheme(String scheme) {
		return schemeToFactory.containsKey(scheme);
	}
}
