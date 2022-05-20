package com.amazon.djk.file;

import com.amazon.djk.expression.SyntaxError;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileQueue {
	private final FileSystem filesys;
	private final List<FileArgs> files;
	private final int initialSize;
	private final String versionId;
	
 	public static class LazyFile {
		private final FileSystem filesys;
		private final FileArgs leaf;
		
		public LazyFile(FileSystem filesys, FileArgs leaf) {
			this.filesys = filesys;
			this.leaf = leaf;
		}
		
		public FileArgs getLeafArgs() {
		    return leaf;
		}

		public InputStream getStream() throws IOException {
			return filesys.getStream(leaf);
		}
	}
	
	public FileQueue(FileSystem filesys, List<FileArgs> dirFiles, Pattern fileRegex) throws IOException {
		this.filesys = filesys;
		this.files = new ArrayList<>();
		// If there is only 1 file, ignore the file-regex
		if (dirFiles.size() == 1) {
			files.add(dirFiles.get(0));
		} else {
			for (FileArgs args : dirFiles) {
				Matcher m = fileRegex.matcher(args.getPath());
				if (m.find()) {
					files.add(args);
				}
			}
		}

		if (dirFiles.size() > 0 && files.size() == 0) {
			String regEx = fileRegex != null ? fileRegex.toString() : "null";
			throw new SyntaxError(String.format("directory contains %d files but none match regex='%s'.  Consider using 'validRegex' as parameter or in source.properties file", dirFiles.size(), regEx));
		}

		// version Ids are the same across all files
		versionId = files.size() > 0 ? filesys.getVersionId(files.get(0)) : "none";

		this.initialSize = files.size();
	}
	
	/**
	 * 
	 * @return the head of the queue or null if empty
	 * @throws IOException
	 */
	public synchronized LazyFile next() throws IOException {
		if (files == null || files.isEmpty()) return null;
		FileArgs leaf = files.remove(files.size() - 1);
		return new LazyFile(filesys, leaf);
	}
	
	public List<FileArgs> getLeafFiles() {
		return files;
	}

	public String getVersionId() {
		return versionId;
	}

	/**
	 * 
	 * @return the current number of files
	 */
	public int currentSize() {
		return files.size();
	}

    public int initialSize() {
        return initialSize;
    }
}
