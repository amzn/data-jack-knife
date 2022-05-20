package com.amazon.djk.format;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import com.amazon.djk.record.Record;

public abstract class FormatWriter {
	private final File dataFile;
	
	public FormatWriter(File dataFile) {
		this.dataFile = dataFile;
	}

	public abstract void writeRecord(Record rec) throws IOException;
	
	public abstract void close() throws IOException;
	
	 /**
     * 
     * @return
     * @throws IOException
     */
    protected OutputStream getStream() throws IOException {
        OutputStream os = new FileOutputStream(dataFile);
        os = new BufferedOutputStream(os, 512 * 1024 * 1);
        if (isGzipped(dataFile)) {
            os = new GZIPOutputStream(os);
        }
        
        return os;
    }
    
    private boolean isGzipped(File file) {
    	return file.getName().endsWith(".gz");
    }
}
