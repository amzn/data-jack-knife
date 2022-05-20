package com.amazon.djk.test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import com.amazon.djk.core.Denormalizer;
import com.amazon.djk.core.Denormalizer.AddMode;
import com.amazon.djk.core.Denormalizer.Context;
import com.amazon.djk.expression.Expression;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.processor.JackKnife;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ProgressReport;
import com.amazon.djk.source.QueueSource;

public class LocalSourceTestSupport {
    public static final String basePath = "build/private/local-source-tests"; // for easy inspection
    private final File baseDir;
    public boolean inited = false;

    public LocalSourceTestSupport() {
        baseDir = new File(basePath);
    }

    private void init() throws IOException {
        if (inited) return;
        FileUtils.deleteDirectory(baseDir);
        baseDir.mkdirs();
        inited = true;
    }

    /**
     * 
     * @param knife
     * @param inrecs the records to sink
     * @param sinkURIFormat the sink djk uri with %s in the file path position, e.g natDB:a,b,c:%s?param=value
     * @param fileExtensionOrNullForDirectory
     * @return the path to the created record source
     * @throws IOException
     * @throws SyntaxError
     */
    public String sinkRecords(JackKnife knife, List<Record> inrecs, String sinkURIFormat, boolean isDir) throws IOException, SyntaxError {
        init();
        QueueSource queue = new QueueSource(); // splittable queue source so multithreaded test
        for (Record rec : inrecs) {
            queue.add(rec, false); // already distinct records from list
        }

        if (sinkURIFormat.indexOf("%s") == -1) {
            throw new RuntimeException("sinkURIFormat must contain a %s corresponding to the postion of the PATH");
        }

        int rand = new Random().nextInt(1000000000);
    	File testBasePath = new File(baseDir, String.format("testBase%d", rand));
        FileUtils.deleteDirectory(testBasePath);
        testBasePath.mkdirs();
        
        // if isDir, just use the new constructed testBase directory, else use a made up file name within that
        String sinkChunk = isDir ? String.format(sinkURIFormat, testBasePath.getAbsolutePath()) :
        	String.format(sinkURIFormat, testBasePath.getAbsolutePath() + "/testFile");
        
        Expression expr = Expression.create(sinkChunk);
        ProgressReport report = knife.execute(queue, expr);
        Record repRec = report.getAsRecord(true);
        
        Denormalizer denormer = new Denormalizer("node", AddMode.CHILD_FIELDS_ONLY, Context.SIMPLE_DENORM);
        denormer.init(repRec);
        
        denormer.next(); // context node
        Record sinkNode = denormer.next();
        
        // see FormatFileSink
        String path = sinkNode.getFirstAsString("path");
        
        if (path == null) {
        	// see NativeDBSink
        	path = sinkNode.getFirstAsString("dbDir");
        }
        
        return path;
    }
    
    /**
     * for convenience
     * @param knife
     * @param path
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    public List<Record> sourceRecords(JackKnife knife, String path) throws IOException, SyntaxError {
        return knife.collectMain(path);        
    }
}
