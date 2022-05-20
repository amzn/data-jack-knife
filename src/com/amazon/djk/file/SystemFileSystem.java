package com.amazon.djk.file;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import com.amazon.djk.expression.ParseToken;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;

/**
 * Returns System.in based stream.
 */
public class SystemFileSystem extends FileSystem {
	public final static String SCHEME = "stdin";
	private boolean available = true;

	public SystemFileSystem(){}

	@Override
	public InputStream getStream(FileArgs leafArgs) throws IOException {
		if (!available) {
			throw new IOException("Only one stdin source allowed per expression");
		}
		
		InputStream is = System.in;
		available = false;
		return new BufferedInputStream(is, bufferNumKilobytes * 1024);
	}

	@Override
	public String scheme() {
		return SCHEME;
	}
	
	@Override
	public void close() throws IOException {
	}

    public List<FileArgs> listDir(FileArgs rootArgs) throws IOException, SyntaxError {
        return Collections.singletonList(new FileArgs(getPathOperator(), new ParseToken("-://")));
    }

    @Override
    public boolean exists(FileArgs leafArgs) throws IOException {
        if (!available) return false;
        available = false;
        return true;
    }

    @Override
    public FileSystemPathOperator getPathOperator() {
        return new Op();
    }
    
    @Description(text = {"Standard in Filesystem. The 'format' parameter is required.",
	"e.g. cat myfile.nv2 | djk stdin://'?format=nv2' devnull"}, contexts={"stdin://'?format=format'"})
    public static class Op extends FileSystemPathOperator {
		public Op() {
			super("stdin");
		} 
    }
}
