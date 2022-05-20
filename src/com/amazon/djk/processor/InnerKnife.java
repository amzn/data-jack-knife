package com.amazon.djk.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.CommaList;
import com.amazon.djk.expression.Expression;
import com.amazon.djk.expression.ExpressionParser;
import com.amazon.djk.expression.InternalMacroOperator;
import com.amazon.djk.expression.MacroOperator;
import com.amazon.djk.expression.Operator;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.file.FileSystem;
import com.amazon.djk.format.FormatOperator;
import com.amazon.djk.format.WriterOperator;
import com.amazon.djk.manual.Display;
import com.amazon.djk.manual.Display.DisplayType;
import com.amazon.djk.manual.ManPage;
import com.amazon.djk.processor.DJKInitializationException.Type;
import com.amazon.djk.record.ThreadDefs;
import com.amazon.djk.report.DisplayMode;
import com.amazon.djk.report.ProgressReport;
import com.amazon.djk.report.ProgressReporter;
import com.amazon.djk.report.ReportConsumer;
import com.amazon.djk.sink.PrintSink;
import com.amazon.djk.source.FormatFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 
 * 
 */
public class InnerKnife {
	private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("jack-knife-thread-%d").build();
    private static final Logger LOG = LoggerFactory.getLogger(InnerKnife.class);

    protected final Set<Class<?>> registered = new HashSet<>();
    protected final HashMap<String, MacroOperator> internalMacros = new HashMap<>();
    protected final Map<String,ManPage> miscManPages = new HashMap<>();
	private ExpressionParser parser = null;    // don't access directly, use getParser()
	protected boolean reportOnce = false;

    private final AtomicReference<ProgressReport> currentProgressReport = new AtomicReference<>();
    private final AtomicBoolean globalForceDone = new AtomicBoolean(false);
    private final AtomicBoolean throwForceDoneException = new AtomicBoolean(false);
    
	/**
	 * In order to enforce One-JackKnife-Per-Thread, initialize requires
	 * CoreDefs to be un-initialized
	 * @throws DJKInitializationException 
	 * 
	 */
	public InnerKnife() throws DJKInitializationException {
		if (CoreDefs.isInitialized()) {
    		throw new DJKInitializationException(Type.MULTIPLE_KNIVES);
		} 
		
		CoreDefs.initialize();
		CoreDefs cdefs = CoreDefs.defs.get();
		
		if (ThreadDefs.isInitialized()) {
    		throw new DJKInitializationException(Type.MULTIPLE_KNIVES);
		}

		ThreadDefs.initialize(cdefs);
	}
	
	public static void deinitialize() {
		CoreDefs.deinitialize();
		ThreadDefs.deinitialize();
	}

    public CollectionContext createCollectionContext(Expression expr) throws IOException {
    	return CollectionContext.create(this, null, expr);
    }
    
    public CollectionContext createCollectionContext(Expression expr, RecordSource upstream) throws IOException {
    	return CollectionContext.create(this, upstream, expr);
    }
    
    public ExecutionContext createExecutionContext(Expression expr) throws IOException {
    	return ExecutionContext.create(this, null, expr);
    }
    
    /**
     * 
     * @param context
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    public CollectionResults collect(CollectionContext context) throws IOException, SyntaxError {
    	ProgressReport report = execute(null, context, DisplayMode.NO_SHOW);
    	return new CollectionResults(report, context);
    }
    
    public CollectionResults collect(Expression expr) throws IOException, SyntaxError {
    	CollectionContext context = createCollectionContext(expr);
    	return collect(context);
    }
    
	public CollectionResults collect(RecordSource upstream, Expression expr) throws IOException {
    	CollectionContext context = createCollectionContext(expr, upstream);
    	ProgressReport report = execute(upstream, context, DisplayMode.NO_SHOW);
    	return new CollectionResults(report, context);
	}

    /**
     * 
     * @param expr
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    public ProgressReport execute(Expression expr) throws IOException, SyntaxError {
    	ExecutionContext context = ExecutionContext.create(this, null, expr);
    	return execute(null, context, DisplayMode.SHOW);
    }
    
    public ProgressReport execute(ExecutionContext context, DisplayMode mode) throws SyntaxError, IOException {
    	return execute(null, context, mode);
    }
    
    /**
     * 
     * @param operand
     * @param expr
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    public ProgressReport execute(RecordSource operand, Expression expr) throws IOException, SyntaxError {
    	ExecutionContext context = ExecutionContext.create(this, operand, expr);
    	return execute(operand, context, DisplayMode.SHOW);
    }
    
    /**
     * 
     * @param context
     * @param mode
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    public ProgressReport execute(RecordSource operand, Expression expr, DisplayMode mode) throws IOException, SyntaxError {
    	ExecutionContext context = ExecutionContext.create(this, operand, expr);
    	return execute(operand, context, mode);
    }
    
    /**
     * The main execute method
     * 
     * @param operand
     * @param expr
     * @param mode
     * @param collect
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    private ProgressReport execute(RecordSource operand, ExecutionContext context, DisplayMode mode) throws IOException, SyntaxError {
    	ProgressReport contextReport = null;
    	try {
            contextReport = executeMainAndReduce(context, mode);

	        RecordSink reportSink = context.getReportSink();
    	    if (reportSink != null) {
                threadedExecute(reportSink, mode);
    	    }
        
    	    /**
    	     * some sinks consume the report about its creation.
    	     */
    	    RecordSink theSink = context.getMainSink();
    	    if (theSink instanceof ReportConsumer) { 
    	        ((ReportConsumer)theSink).consume(contextReport);
    	    }
    	}
        
    	finally {
    	    getParser().close();
    	}
    	
        return contextReport;
    }
    
    /**
     * allows an external thread to terminate early. 
     */
    
    /**
     * allows an external thread to terminate early
     * @param throwException if true, causes throwing of ForceDoneException at termination
     */
    public void forceDone(boolean throwException) {
    	globalForceDone.set(true);
    	throwForceDoneException.set(throwException);
    }
    
    /**
     * 
     * @param context
     * @param mode
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    private ProgressReport executeMainAndReduce(ExecutionContext context, DisplayMode mode) throws IOException, SyntaxError {
    	ProgressReporter reporter = null;
    	ProgressReport contextReport = null;
    	
    	try {
    		RecordSink theSink = context.getMainSink();

    		// override the mode to NO_SHOW when printing to terminal
    		mode = reportOnce ? DisplayMode.SHOW_ONCE :
                (context.isUsingPrintSink() && mode != DisplayMode.FORCE_SHOW) ? 
                DisplayMode.NO_SHOW : mode; 

    		contextReport = context.getReport();
    		currentProgressReport.set(contextReport);		

    		int refreshSecs = CoreDefs.get().getReportRefreshSecs();
    		reporter = new ProgressReporter(contextReport, refreshSecs, mode);
    		reporter.start(); // separate thread does reporting

    		// blocks till end
    		threadedExecute(theSink, mode);

    		// execute reduction, this could be recursive but currently NOT
    		executeReduce(context, mode);
    	}
    	
    	catch (IOException e) {
            shutdownReporter(reporter);
            throw e;
    	}

        shutdownReporter(reporter);
        return contextReport;
    }
    
    private void shutdownReporter(ProgressReporter reporter) throws IOException {
    	try {
    		reporter.stopThread();
    		reporter.interrupt(); // will display one last time.
    		reporter.join();
    	} catch (InterruptedException e) {
    		throw new IOException(e);
    	}
    }
    
    /**
     * map reduce instances (sources) to their reduce expressions and run them.
     * 
     * @param context
     * @param mainSink
     * @throws IOException 
     */
    private void executeReduce(ExecutionContext context, DisplayMode mode) throws IOException {
    	Map<CommaList,RecordSink> rsinks = context.getReduceSinks();
    	for (RecordSink rsink : rsinks.values()) {
    		threadedExecute(rsink, rsink instanceof PrintSink ? DisplayMode.NO_SHOW: mode);
    	}
    }

    /**
     * 
     * @return the current progress report, could be null
     */
    public ProgressReport getCurrentProgressReport() {
    	return currentProgressReport.get();
    }
    
    /**
     * 
     * @param theSink
     * @return exhausted strands
     * @throws IOException 
     * @throws SyntaxError 
     */
    List<RecordSink> threadedExecute(RecordSink theSink, DisplayMode mode) throws IOException, SyntaxError {
    	// multi-threaded loads maps outside the main execution
    	InnerSinkDrainer.drain(this, theSink);
    	
		// start threads for executing threadSinks
		ExecutorService pool = Executors.newFixedThreadPool(CoreDefs.get().getNumSinkThreads(), threadFactory);

		// prepare each call
		globalForceDone.set(false);
		throwForceDoneException.set(false);
		
    	List<SinkDrainer> drainers = SinkDrainer.getDrainers(theSink, globalForceDone);		
		for (SinkDrainer drainer : drainers) {
			pool.execute(drainer);
		}
		
    	List<RecordSink> exhaustedStrands = new ArrayList<>();
    	AtomicReference<IOException> firstException = new AtomicReference<>(null);
    	
    	while (!drainers.isEmpty()) {
    	    Iterator<SinkDrainer> iter = drainers.iterator();
            while (iter.hasNext()) {
                SinkDrainer drainer = iter.next();
                 if (drainer.isDone()) {
                	 firstException.compareAndSet(null, drainer.getException());
                	 exhaustedStrands.add(drainer.getSink());
                	 iter.remove();
                 }
    		}
    		
            try {
    	    	Thread.sleep(50);
    	    } catch (InterruptedException e) { }
    	}
    	
    	// done - shutdown
    	pool.shutdown();
    	while (!pool.isTerminated()) { }
    	
    	// something went wrong in normal execution
    	if (firstException.get() != null) {
    		throw firstException.get();
    	}
    	
    	// we were forced
    	if (globalForceDone.get() && throwForceDoneException.get()) {
    		throw new ForceDoneException();
    	}
    	
    	return exhaustedStrands;
    }

    /**
     * 
     * @return a map of the miscellaneous man pages.
     */
    public Map<String,ManPage> getMiscManPages() {
    	return miscManPages;
    }
    
    /************************************************************************
     * 
     * registration section
     * 
     * every component registered lives inside the parser
     * 
     ***********************************************************************/
    
    public void registerInternalMacro(String name) {
        try {
            final List<String> rawMacroLines = JackKnife.getResourceAsLines(this, String.format("%s.djk", name));
            MacroOperator op = new InternalMacroOperator(name, rawMacroLines);
            internalMacros.put(op.getName(), op);
        }
        
        catch (IOException e) {
            LOG.error("error constructing component:" + e);
        }
    }
    
    public void registerManPage(Class<? extends Operator> clazz) {
        registered.add(clazz);
    }
    
    public void registerOp(Class<? extends Operator> clazz) {
        registered.add(clazz);
    }
    
    public void registerFileSystem(Class<? extends FileSystem> clazz) {
        registered.add(clazz);
    }

    public void registerJackKnife(Class<? extends JackKnife> clazz) throws IOException {
        if (this.getClass() == clazz) return; // no no no!  don't surround yourself with yourself
        
        CoreDefs cdefs = CoreDefs.get(); // get the defs of 'this' knife.
        CoreDefs.deinitialize(); // deinitialize for the constructor below
        ThreadDefs.deinitialize(); // deinitialize the per Thread wrapper
        
        try {
            JackKnife knife = clazz.newInstance();
            Iterator<Class<?>> iter = knife.registered.iterator();
            while (iter.hasNext()) {
                registered.add(iter.next());
            }

            for (Map.Entry<String, MacroOperator> e : knife.internalMacros.entrySet()) {
                internalMacros.put(e.getKey(), e.getValue());
            }
        } 
        
        catch (InstantiationException | IllegalAccessException e) {
            LOG.error("error constructing component:" + e);
        }
        
        // put the defs back
        CoreDefs.defs.set(cdefs);
        ThreadDefs.initialize(cdefs);
    }

    /**
     * 
     * @param manPage
     */
    private void addManPage(ManPage manPage) {
    	int size = miscManPages.size();
    	miscManPages.put(manPage.getName(), manPage);
    	if (miscManPages.size() != size + 1) {
            throw new RuntimeException("Improperly configured jackknife." +  
                    "Man page name collision for '" + manPage.getName() +
                    "' from '" + manPage.getClass().getSimpleName() + "'");
        }
    }
    
    /**
     * accessor provides lazy instantiation of components
     * 
     * @return the expression parser
     * @throws IOException
     */
    public ExpressionParser getParser() throws IOException {
        if (parser != null && registered.isEmpty()) return parser;
        
        Iterator<Class<?>> opsIter = registered.iterator();
        Map<String,PipeOperator> pipeOps = new TreeMap<>();
        Map<String,SourceOperator> sourceOps = new TreeMap<>();
        FormatFactory sourceFactory = new FormatFactory();

        try {
            while (opsIter.hasNext()) {
                Class<?> clazz = opsIter.next();
                // instantiate the Operator
                Object obj = clazz.newInstance();
                
                if (obj instanceof ManPage) {
                	addManPage((ManPage)obj);
                }
                
                // sinks and pipes go here
                else if (obj instanceof PipeOperator) {
                    PipeOperator pop = (PipeOperator)obj;
                    int size = pipeOps.size();
                    pipeOps.put(pop.getName(), pop);                    
                    if (pipeOps.size() != size + 1) {
                        throw new RuntimeException("Improperly configured jackknife." +  
                                "Predicate name collision for '" + pop.getName() + 
                                "' from '" + pop.getClass().getSimpleName() + "'");
                    }
                }
                
                else if (obj instanceof WriterOperator) {
                	sourceFactory.addWriterOperator((WriterOperator)obj);
                }

                else if (obj instanceof FormatOperator) {
                	sourceFactory.addFormatOperator((FormatOperator)obj);
                }
                
                // non-format source operators go here
                else if (obj instanceof SourceOperator) {
                	SourceOperator sop = (SourceOperator)obj;
                    if (sourceOps.get(sop.getName()) != null) {
                        throw new RuntimeException("Improperly configured jackknife." +  
                                "Predicate name collision for '" + sop.getName() + 
                                "' from '" + sop.getClass().getSimpleName() + "'");
                    }
                    
                    sourceOps.put(sop.getName(), sop);
                }
                
                else if (obj instanceof FileSystem) {
                    sourceFactory.addFileSystem((FileSystem)obj);
                }
            }
        }
        
        catch (InstantiationException | IllegalAccessException e) {
            LOG.error("error constructing component (is the operator constructor argumentless?):" + e);
            throw new IOException("error constructing component (is the operator constructor argumentless?): " + e);
        }

        for (Map.Entry<String, MacroOperator> e : internalMacros.entrySet()) {
            pipeOps.put(e.getKey(), e.getValue());
        }
        
        parser = new ExpressionParser(CoreDefs.get().getPropertiesNamespace(), sourceFactory, sourceOps, pipeOps);
        
        registered.clear(); // so empty        
        return parser;
    }
 
    public ManPage getManPage(String topic) throws IOException {
        return miscManPages.get(topic);
    }
}
