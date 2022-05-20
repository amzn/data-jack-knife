package com.amazon.djk.file;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.ParseToken;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

public class LocalFileSystem extends FileSystem {
	public final static String LOCAL_SCHEME = "file";

	@Override
	public List<FileArgs> listDir(FileArgs args) throws IOException, SyntaxError {
        List<FileArgs> leaves = new ArrayList<>();
		
		File path = new File(args.getPath());
		if (path.exists()) {
			if (path.isDirectory()) {
				for (File file : path.listFiles()) {
					if (file.isDirectory()) { // no recursion now
						continue;
					}

					leaves.add(createLeafFileArgs(file));
				}
			}
			
			else {
			    leaves.add(args);  // single file
			}
		}
		
		return leaves;
	}
	
	private FileArgs createLeafFileArgs(File file) throws IOException, SyntaxError {
		String uri = String.format("%s://%s", LOCAL_SCHEME, file.getAbsolutePath());;
		ParseToken token = new ParseToken(uri);
		return new FileArgs(getPathOperator(), token);		
	}
	
	@Override
	public boolean exists(FileArgs leafArgs) throws IOException {
	    File file = new File(leafArgs.getPath());
	    return file.exists();
	}
	
    @Override
    public InputStream getStream(FileArgs leafArgs) throws IOException {
        File file = new File(leafArgs.getPath());
        if (!file.exists()) {
			return null;
		}
		
		InputStream is = null;
		if (file.getName().endsWith(".gz")) {
			is = new FileInputStream(file);
			is = new GZIPInputStream(is, 32 * 1024);
            is = new BufferedInputStream(is, bufferNumKilobytes * 1024);
		}

		else if (file.getName().endsWith(".bz2")) {
			is = new FileInputStream(file);
			is = new BZip2CompressorInputStream(is);
			is = new BufferedInputStream(is, bufferNumKilobytes * 1024);
		}
		
		else { // not gzipped
			is = new FileInputStream(file);
			is = new BufferedInputStream(is, bufferNumKilobytes * 1024);
		}

		return is;
	}

	@Override
	public String scheme() {
		return "file";
	}
	
	@Override
	public void close() throws IOException {
	}

	@Override
	public FileSystemPathOperator getPathOperator() {
	    return new Op();
	}

	@Description(text = {"Local Filesystem. 'file://' is optional."}, contexts={"file://PATH", "PATH"})
	@Arg(name=FileOperator.PATH_ARG, gloss="path to local format source.", type=ArgType.STRING)
    public static class Op extends FileSystemPathOperator {
		public Op() {
			super("file");
		} 
	}
}
