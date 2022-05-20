package com.amazon.djk.file;

import com.amazon.djk.expression.SyntaxError;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * abstract class for support various file systems
 */
public abstract class FileSystem {
	protected int bufferNumKilobytes = 1024; // default 1MB
	protected static final String NONE = "none";
	
	public void setStreamBufferSize(int numKilobytes) {
		this.bufferNumKilobytes = numKilobytes;
	}

	/**
	 * 
	 * @param number
	 * @return
	 */
	public static String getNumberedFileName(String name, int number, String suffix) {
	    if (name == null) return null;
	    return String.format("%s-%05d.%s", name, number, suffix);
	}

	/**
	 * 
	 * @param leafArgs
	 * @return true if a resource corresponding to leafArgs exists
	 * @throws IOException
	 */
	public abstract boolean exists(FileArgs leafArgs) throws IOException;
	
	/**
	 * 
	 * @param leafArgs
	 * @return
	 * @throws IOException
	 */
    public abstract InputStream getStream(FileArgs leafArgs) throws IOException;
	
	/**
	 * if rootArgs is a directory, method returns list of dir contents as FileArgs
	 * 
	 * @param rootArgs
	 * @throws IOException
	 * @throws SyntaxError
	 */
	public abstract List<FileArgs> listDir(FileArgs rootArgs) throws IOException, SyntaxError;
	
	/**
	 * 
	 * @return the name of the scheme of this file system
	 */
	public abstract String scheme();
	
	/**
	 * 
	 * @throws IOException
	 */
	public abstract void close() throws IOException;

	/**
	 * gives the oportunity to have file system parameters. 
	 * 
	 * 
	 * @return the path operator for this file system.
	 */
    public abstract FileSystemPathOperator getPathOperator();

	/**
	 * Returns the version id of current file system. If version is not
	 * supported, null is returned
	 */
	public String getVersionId(FileArgs leafArgs) throws IOException {
		return NONE;
	}

	/**
     * The PATH argument is hardwired in FileOperator.  Subclasses are required
     * to annotate @Arg FileOperator.PATH_ARG as STRING.  This class is used at
     * the time when the format of the source is still unknown. 
     *
     */
    public static class FileSystemPathOperator extends FileOperator {
        public FileSystemPathOperator(String name) {
            super(name, "unused regex");
        }
    }
}
