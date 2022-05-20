package com.amazon.djk.natdb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.file.SourceProperties;
import com.amazon.djk.format.WriterOperator;
import com.amazon.djk.keyed.KeyedSink;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.misc.Hashing;
import com.amazon.djk.processor.FieldDefs;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;
import com.amazon.djk.record.RecordIO.IOBytes;
import com.amazon.djk.record.ThreadDefs;
import com.amazon.djk.report.GraphDisplay;
import com.amazon.djk.report.ProgressData;
import com.amazon.djk.report.ProgressReport;
import com.amazon.djk.report.ReportConsumer;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;
import com.amazon.djk.sink.FileSinkHelper;

/**
 * 
 */
@ReportFormats2(headerFormat="file://<dir>%s?keys=%s&format=ndb&child=%s",
    lineFormats={"waiting=%d loading=%d sorting=%d writing=%d"})
public class NativeDBSink extends KeyedSink implements ReportConsumer {
    private static final Logger logger = LoggerFactory.getLogger(NativeDBSink.class);
    public final static int DEFAULT_NUM_BUCKETS = 80;
    public static final String DEFAULT_COUNT_PARAM = "false";
    public static final String COUNT = "count";
    public static final String FORMAT = "natdb";

    private final boolean addCount;
    private final TempFileWriter[] writers;
    private final RecordFIFO[] transFIFOs;
    protected final BlockingQueue<Integer> bucketQueue;
    
    protected final AtomicLong numUniq;

    @ScalarProgress(name="child")
    protected final Field subName;
    
    @ScalarProgress(name="waiting", aggregate=AggType.NONE)
    protected volatile int numWaiting = 0;
    @ScalarProgress(name="loading")
    protected volatile int numLoading = 0;
    @ScalarProgress(name="sorting")
    protected volatile int numSorting = 0;
    @ScalarProgress(name="writing")
    protected volatile int numWriting = 0;

    @ScalarProgress(name="dir")
    protected final File dbDir;
    @ScalarProgress(name="keys")
    private final String keyString;
    protected final boolean onePerKey;
    
    private final int numBuckets;
    private final int[] bucketMaxGroupSize;
    private final int[] bucketMaxByteSize;
    
    private final IOBytes tempBytes = new IOBytes();
    
    private final int numSortThreads;
    
    // set in drain
    private AtomicBoolean forceDone = new AtomicBoolean(false);
    
    public NativeDBSink(OpArgs args, FileSinkHelper info) throws IOException {
        super(null, args);
        this.dbDir = new File(info.absolutePath());
        
        writers = getWriterThreads(dbDir);
        keyString = StringUtils.join(keys.getFieldNames(), ",");
        numBuckets = ThreadDefs.get().getNumSortBuckets();
        bucketQueue = new LinkedBlockingQueue<>();
        numUniq = new AtomicLong(0);
        subName = (Field)args.getParam(MemDBSource.SUBNAME_PARAM);
        onePerKey = subName.getName().equals("NONE");

        addCount = (Boolean)args.getParam(COUNT);
        
        transFIFOs = new RecordFIFO[numBuckets];
        bucketMaxGroupSize = new int[numBuckets];
        bucketMaxByteSize = new int[numBuckets];
        
        numSortThreads = ThreadDefs.get().getNumSortThreads();
    }
    
    private NativeDBSink(NativeDBSink root, OpArgs args) throws IOException {
        super(root, args);
        dbDir = root.dbDir;
        keyString = StringUtils.join(keys.getFieldNames(), ",");
        subName = (Field)args.getParam(MemDBSource.SUBNAME_PARAM);
        numBuckets = root.numBuckets;
        transFIFOs = new RecordFIFO[numBuckets];
        onePerKey = root.onePerKey;
        addCount = (Boolean)args.getParam(COUNT);
        
        // shared with root, threadsafe
        writers = root.writers;
        numUniq = root.numUniq;
        bucketQueue = root.bucketQueue;
        bucketMaxGroupSize = root.bucketMaxGroupSize;
        bucketMaxByteSize = root.bucketMaxByteSize;
        
        numSortThreads = ThreadDefs.get().getNumSortThreads();
    }
    
    private static TempFileWriter[] getWriterThreads(File dbDir) throws IOException {
    	int numSortBuckets = ThreadDefs.get().getNumSortBuckets();
        TempFileWriter[] writers = new TempFileWriter[numSortBuckets];
        for (int i = 0; i < numSortBuckets; i++) {
            writers[i] = new TempFileWriter(dbDir, i);
            writers[i].start();
        }
        
        return writers;
    }
    
    @Override
    public ProgressData getProgressData() {
        int num = 0;
        Iterator<Integer> els = bucketQueue.iterator();
        while (els.hasNext()) {
            if (els.next() != -1) num++;
        }
        
        numWaiting = num;
        return super.getProgressData();
    }

    @Override
    public Object replicate() throws IOException {
    	// TODO: WAIT! this should never happen, test and remove
        if (getNumInstances() >= ThreadDefs.get().getNumSinkThreads()) {
            return null;
        }

        return new NativeDBSink(this, args);
    }
    
    @Override
    public void drain(AtomicBoolean forceDone) throws IOException {
    	this.forceDone = forceDone;
    	super.drain(forceDone);
    }
    
    @Override
    public void store(Record keyRecord, Record valueRecord) throws IOException {
        int bucketNo = (int)(Hashing.hash63(keyRecord) % numBuckets);
        TempFileWriter writer = writers[bucketNo];
        
        RecordFIFO fifo = transFIFOs[bucketNo];
        if (fifo == null) {
            fifo = new RecordFIFO();
            transFIFOs[bucketNo] = fifo;
        }

        valueRecord.writeCompressedTo(tempBytes);
        keyRecord.addField(FieldDefs.INTERNAL_FIELD_NAME, tempBytes); 
        
        fifo.add(keyRecord);        
        if (fifo.byteSize() > 1024 * 1024) {
            try {
                writer.put(fifo);
                transFIFOs[bucketNo] = null;
            } catch (InterruptedException e) {
                logger.error("broken ndb build", e);
                throw new IOException("broken ndb build");
            }
        }
    }
    
    /**
     * each thread finishes transfering non-empty fifos
     * @throws IOException 
     */
    private void finishTransfers() throws IOException {
        for (int i = 0; i < numBuckets; i++) {
            RecordFIFO fifo = transFIFOs[i];
            if (fifo == null) continue;
            
            TempFileWriter writer = writers[i];
            try {
                writer.put(fifo);
                transFIFOs[i] = null;
            } catch (InterruptedException e) {
                logger.error("broken ndb build", e);
                throw new IOException("broken ndb build");                
            }
        }
        
        logger.info("instanceNo=" + getInstanceNo() + " finished transfering FIFOs");
    }

    /**
     * the last thread prepares for sorting
     * @throws IOException 
     */
    private void prepareSorting() throws IOException {
        logger.info("instanceNo=" + getInstanceNo() + " preparing sorting");
        for (TempFileWriter writer : writers) {
            try {
                writer.finish();
                writer.join();
            } catch (InterruptedException e) {
                logger.error("broken ndb build", e);
                throw new IOException("broken ndb build");   
            }
        }
        
        logger.info("instanceNo=" + getInstanceNo() + " tempFileWriting complete");
        
        int numSinks = getNumInstances();
        int numSorters = Math.min(numSinks, numSortThreads); 

        int toKill = numSinks - numSorters;
        logger.info(String.format("instanceNo=%d numSinks=%d numSorters=%d toKill=%d numBuckets=%d",
                getInstanceNo(), numSinks, numSorters, toKill, numBuckets));
        
        try {
            for (int i = 0; i < toKill; i++) {
                bucketQueue.put(-1); // poison pill
            }
            
        	for (int i = 0; i < numBuckets; i++) {
        		bucketQueue.put(i); // work to be done
        	}
        	logger.info("instanceNo=" + getInstanceNo() + " initial bucketQueueSize=" + bucketQueue.size());
        	
        	for (int i = 0; i < numSorters; i++) {
        		bucketQueue.put(-1); // poison pill
        	}
        }
        
        catch (InterruptedException e) {
        	logger.error("BROKEN NDB BUILD", e);
            throw new IOException("BROKEN NDB BUILD");
        }
        
        logger.info("instanceNo=" + getInstanceNo() + " sort preparation complete");
    }
    
    @Override
    public void consume(ProgressReport report) throws IOException {
        File reportFile = new File(dbDir, "report.txt");
        GraphDisplay display = new GraphDisplay();
        report.display(display);
        display.render(reportFile);


        Record originReport = report.getAsRecord(false);
        FileSinkHelper.persistOriginReport(originReport, dbDir);
    }
    
    @Override
    public void close() throws IOException {
        finishTransfers();
        int numKeyFields = keys.getAsFieldList().size();
        
        if (isLastSync1.arriveAndIsLast()) {
           prepareSorting();
        }
        
        try {
            while (!forceDone.get()) {
                Integer bucketNo = bucketQueue.take();
                if (bucketNo == -1) {
                    break; // we're done
                }

                logger.info("instanceNo=" + getInstanceNo() + " sorting bucketNo=" + bucketNo);
                
                numLoading++;
                BucketSorter  sorter = new BucketSorter(dbDir, subName, numKeyFields, addCount, bucketNo);
                numLoading--;

                numSorting++;
                sorter.sort();
                numSorting--;
                
                numWriting++;
                sorter.write();
                numWriting--;
                
                numUniq.addAndGet(sorter.getNumWritten());                
                ((NativeDBSink)root()).bucketMaxByteSize[bucketNo] = sorter.getMaxGroupUncompressedByteSize();
                ((NativeDBSink)root()).bucketMaxGroupSize[bucketNo] = sorter.getMaxGroupSize();
            }
        }
        
        catch (Exception e) {
            logger.error("BUILD FAILURE", e);
            throw new IOException(e);
        }

        logger.info("instanceNo=" + getInstanceNo() + " db complete");
        
        if (isLastSync2.arriveAndIsLast()) {
            writeProperties();
        }

        super.close();
    }
    
    protected void writeProperties() throws IOException {
        // the last sync writes the properties 
        Map<String,String> extras = new HashMap<>();
        extras.put("groupOut", subName.getName());
        extras.put("numBuckets", Integer.toString(numBuckets));
        extras.put("bucketMaxGroupSizes", intArrayToString(bucketMaxGroupSize));
        extras.put("bucketMaxByteSizes", intArrayToString(bucketMaxByteSize));
        SourceProperties.write(dbDir, numUniq.get(), FORMAT, NativeDBFileParser.STREAM_FILE_REGEX, keys.getFieldNames(), extras);
    }
    
    private static String intArrayToString(int[] array) {
        StringBuilder sb = new StringBuilder();
        
        for (int i : array) {
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(i);
        }
        
        return sb.toString();
    }
 
    @Description(text={"Provides persistant storage and lookup of records, mapped by KEYS, i.e. only the first record instance is mapped."})
    @Arg(name="PATH", gloss="directory path", type=ArgType.STRING, eg="/tmp/mydb")
    @Arg(name = "KEYS", gloss = "comma separated list of key fields.", type = ArgType.FIELDS, eg="id")
    @Param(name = COUNT, gloss = "If true, adds a child count field with the number of original entries of this KEY.", type = ArgType.BOOLEAN, defaultValue = DEFAULT_COUNT_PARAM)
    @Param(name= WriterOperator.OVERWRITE_PARAM, gloss="If true, previous data will be overwritten.", type=ArgType.BOOLEAN, defaultValue = "false")
    @Gloss(entry = "numSortBuckets", def = "System.property. default=80.  Try upping this number if sort files grow greater than 2GB")
    @Gloss(entry = "numSortThreads", def = "System.property. default=32.  Adjust this number to accommodate memory constraints")
    public static class MapOp extends PipeOperator {
        private final boolean isMap;
        
        public MapOp() {
            super("mapDB:KEYS:PATH");
            isMap = true;
        }
        
        /**
         * grouping constructor
         * @param usage
         */
        public MapOp(String usage) {
            super(usage);
            isMap = false;
        }

        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            if (isMap) {
                args.addAnnotationLessParam(MemDBSource.SUBNAME_PARAM, new Field(MemDBSource.NO_GROUP));
            }

            String path = (String)args.getArg("PATH");
            boolean overwrite = (Boolean)args.getParam(WriterOperator.OVERWRITE_PARAM);
            FileSinkHelper info = new FileSinkHelper(path, FORMAT, overwrite);
            File dir = new File(info.absolutePath());
            if (!dir.exists()) {
            	if (!dir.mkdirs()) {
            		throw new IOException("unable to create directory: " + info.absolutePath());
            	}
            }
            
            //File dir = help.getPath();
            return new NativeDBSink(args, info).addSource(operands.pop());
        }
    }
    
    @Description(text={"Provides persistant storage and lookup of records, grouped by KEYS."})
    @Param(name = MemDBSource.SUBNAME_PARAM, gloss = "name of the subrecord field within a group record.  However, if child=NONE, only the first record instance is mapped to the KEY using a flat structure. (i.e. no subrecord).", type = ArgType.FIELD, eg="sub", defaultValue = MemDBSource.DEFAULT_SUBNAME)
    public static class GroupOp extends MapOp {
        public GroupOp() {
            super("groupDB:KEYS:PATH");
        }        
    }
 }
