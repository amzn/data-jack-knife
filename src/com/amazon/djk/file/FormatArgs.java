package com.amazon.djk.file;

import com.amazon.djk.expression.ParseToken;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.format.FormatException;
import com.amazon.djk.format.NV2FormatParser;
import com.amazon.djk.format.PushbackLineReader;
import com.amazon.djk.record.Record;
import com.amazon.djk.sink.FileSinkHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * FormatArgs are FileArgs resolved to particular format (often by virtue of
 * information in the files) The format might also come from the properties.
 *
 */
public class FormatArgs extends FileArgs {
    private static Logger LOG = LoggerFactory.getLogger(FormatArgs.class);
    public final static String SOURCE_PROP_FILE = "source.properties";
    public final static String REPORT_NAT_FILE = "origin.report"; // avoid nat extension
    public static final String SCHEME_SUFFIX = "://";
    private final List<FileArgs> files;
    private SourceProperties sourceProps;
    private final FileSystem filesys;
    private final String format;

    public static FormatArgs create(FileSystems fileSystems, ParseToken token, String formatHint, boolean asSource) throws SyntaxError, IOException {
        String scheme = token.isScheme() ? token.getOperator() : LocalFileSystem.LOCAL_SCHEME;
        FileSystem filesys = fileSystems.getFileSystem(scheme);
        return create(filesys, token, formatHint, asSource);
    }
    
    private static FormatArgs create(FileSystem filesys, ParseToken token, String formatHint, boolean asSource) throws SyntaxError, IOException {
        FileArgs fileArgs = new FileArgs(filesys.getPathOperator(), token);
        List<FileArgs> leaves = filesys.listDir(fileArgs);
        
        if (asSource && sourcePropertiesFileExists(leaves)) {
            return new FormatArgs(fileArgs, leaves, filesys);
        }
        
        String overridingFormat = (String)fileArgs.getParam(FileOperator.FILE_FORMAT_PARAM);
        String format = overridingFormat != null ? overridingFormat : formatHint;
        
        if (format == null) {
            throw new SyntaxError("unknown format for " + fileArgs.getURI());
        }
        
        if (!asSource) { // as sink
        	return new FormatArgs(fileArgs, leaves, filesys, format);
        }
        
        // must be a single file, if it exists, requires format param
        if (leaves.isEmpty()) {
            if (!filesys.exists(fileArgs)) {
                throw new FileNotFoundException();
            }

            return new FormatArgs(fileArgs, filesys, format);
        }
        
        else {
        	return new FormatArgs(fileArgs, leaves, filesys, format);
        }
    }

    /**
     * retrieve format args for a local filesystem directory
     * 
     * @param token
     * @return
     * @throws SyntaxError
     * @throws IOException
     */
    public static FormatArgs createLocal(ParseToken token) throws SyntaxError, IOException {
        LocalFileSystem filesys = new LocalFileSystem();
        return create(filesys, token, null, true); // no hint, asSource=true
    }
    
    /**
     * directory constructor 
     * 
     * @param root the directory
     * @param leaves files of the directory
     * @param filesys
     * @throws SyntaxError
     * @throws IOException
     */
    private FormatArgs(FileArgs root, List<FileArgs> leaves, FileSystem filesys) throws SyntaxError, IOException {
        super(filesys.getPathOperator(), root.getToken());
        this.filesys = filesys;
        this.files = leaves;

        FileArgs propsFile = removeFile(leaves, SOURCE_PROP_FILE);
        if (propsFile == null) {
            throw new SyntaxError("directory missing source.properties file");
        }

        FileArgs reportFile = removeFile(leaves, REPORT_NAT_FILE);
        Record report = getOriginReportRecord(reportFile);
        
        InputStream propsStream = filesys.getStream(propsFile);
        Properties fileProps = new Properties();
        fileProps.load(propsStream);
        
        String regex = fileProps.getProperty(SourceProperties.SOURCE_PROP_FORMAT_REGEX);
        if (regex != null) {
        	removeNonValidStreams(Pattern.compile(regex), leaves);
        }
        
        sourceProps = new SourceProperties(this, fileProps, report);
        format = sourceProps.getSourceFormat();
    }

    /**
     * file constructor 
     * @param root
     * @param leaves
     * @param filesys
     * @param format
     * @throws SyntaxError
     * @throws IOException
     */
    private FormatArgs(FileArgs root, List<FileArgs> leaves, FileSystem filesys, String format) throws SyntaxError, IOException {
        super(filesys.getPathOperator(), root.getToken());
        this.filesys = filesys;
        this.files = (leaves != null) ? leaves : Collections.singletonList(root);
        sourceProps = new SourceProperties(this, format);
        this.format = format;
    }

    private FormatArgs(FileArgs root, FileSystem filesys, String format) throws SyntaxError, IOException {
        this(root, null, filesys, format);
    }

    public FileQueue getInputStreamQueue(Pattern defaultFormatRegex) throws IOException {
        // command param overrides
        String regexParam = (String)getParam(FileOperator.FILE_VALID_REGEX_PARAM);
        if (regexParam != null) {
            Pattern p = Pattern.compile(regexParam);
            return new FileQueue(filesys, files, p); // take the param override
        }

        // then source.properties
        Pattern regex = sourceProps.getValidRegex();

        // then default format
        return new FileQueue(filesys, files, regex != null ? regex : defaultFormatRegex);
    }
    
    /**
     * 
     * @return
     * @throws IOException
     */
    public SourceProperties getSourceProperties() throws IOException {
        return sourceProps;
    }
    
    public int getNumStreams(Pattern streamFileRegex) throws IOException {
        if (streamFileRegex == null) return files.size();
        
        int num = 0;
        for (FileArgs args : files) {
            Matcher m = streamFileRegex.matcher(args.getPath());
            if (m.find()) {
                num++;
            }
        }
        
        return num;
    }
    
    public String getFormat() {
    	return format;
    }
        
    /**
     * 
     * @param leaves
     * @return
     * @throws IOException
     */
    private FileArgs removeFile(List<FileArgs> leaves, String file) throws IOException {
        Iterator<FileArgs> iter = leaves.iterator();
        while (iter.hasNext()) {
            FileArgs args = iter.next();
            String path = args.getPath();
            if (path.endsWith(file)) {
                iter.remove();
                return args;
            }             
        }

        return null;
    }
    
    /**
     * 
     * @param streamFileRegex
     * @param leaves
     * @throws IOException
     */
    public void removeNonValidStreams(Pattern streamFileRegex, List<FileArgs> leaves) throws IOException {
    	Iterator<FileArgs> iter = leaves.iterator();
    	while (iter.hasNext()) {
    		FileArgs args = iter.next();
    		Matcher m = streamFileRegex.matcher(args.getPath());
    		if (!m.find()) {
    			iter.remove();
    		}
    	}
    }
    
    public static boolean sourcePropertiesFileExists(List<FileArgs> leaves) throws IOException {
        Iterator<FileArgs> iter = leaves.iterator();
        while (iter.hasNext()) {
            FileArgs args = iter.next();
            String path = args.getPath();
            if (path.endsWith(SOURCE_PROP_FILE)) {
                return true;
            }             
        }

        return false;
    }
    
    private Record getOriginReportRecord(FileArgs reportFile) {
    	if (reportFile == null) return null;
    	try (InputStream is = filesys.getStream(reportFile)) {
            return FileSinkHelper.getOriginReport(is);
        }

    	catch (IOException e) {
            LOG.warn(String.format("could not read origin file '%s'", FormatArgs.REPORT_NAT_FILE), e);
    	    return null;
        }
    }
}
