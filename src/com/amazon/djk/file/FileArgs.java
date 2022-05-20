package com.amazon.djk.file;

import java.io.IOException;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParseToken;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.file.FileSystem.FileSystemPathOperator;
import com.google.common.base.Strings;

/**
 * Unlike OpArgs (used by Pipes/Sinks and named sources) FileArgs does not
 * and in fact can not require an Operator at construction time.
 * Streams sources are different than others because we can't resolve
 * to an operator BEFORE getting the args.  This is so because the
 * streams themselves may be used to map to the format (i.e. via
 * formatRegex or the streams.properties file)  Therefore we use 
 * a generic Op to define a PATH usage. 
 *
 */
public class FileArgs extends OpArgs {
	public static final String SCHEME_SUFFIX = "://";
	private final String scheme;
	private final String path;
	private final String uriString;
	
	public FileArgs(FileSystemPathOperator fileSystemPathOp, ParseToken token) throws SyntaxError, IOException {
        super(fileSystemPathOp, token);
        scheme = token.isScheme() ? token.getOperator() : LocalFileSystem.LOCAL_SCHEME;
        path = getPath(token.toString());
        uriString = getURI();
	}
	
	/**
	 * 
	 * @param uri as [scheme://]path?params
	 * @throws SyntaxError
	 * @throws IOException
	 */
	public FileArgs(FileSystemPathOperator fileSystemPathOp, String uri) throws SyntaxError, IOException {
        this(fileSystemPathOp, new ParseToken(uri));
    }

	public static String getPath(String stringURI) {
        int slashSlash = stringURI.indexOf(SCHEME_SUFFIX);
        if (slashSlash != -1) {
            stringURI = stringURI.substring(slashSlash+3);
        }

		stringURI = stringURI.replaceFirst("\\?.*", ""); // kill params
		return stringURI.replaceFirst("/$", ""); // kill trailing slash
	}

	/**
	 * fileArgs only provide params as Strings
	 * 
	 * @param name
	 * @return the string value associated with name
	 *
	public String getParamAsString(String name) {
	    return paramMap.get(name);
	}*/
	
	public String getScheme() {
		return scheme;
	}
	
	public String getPath() throws IOException {
		return path;
	}
	
	public String getURI() throws IOException {
	    String s = getParamsAsString(); //either ignore the undefined params here -- or figure out how to deal with them downstream.
	    return String.format("%s%s%s%s",
	            scheme,
	            SCHEME_SUFFIX,
	            getPath(),
				Strings.isNullOrEmpty(s) ? "" : "?" + s);
	}
	
	@Override
	public String toString() {
	    return uriString;
	}
}
