package com.amazon.djk.processor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Display;
import com.amazon.djk.processor.DJKInitializationException.Type;
import com.amazon.djk.processor.KnifeProperties.Namespace;

/**
 * This singleton thread-local class is instantiated in the JackKnife thread and shared
 * across worker threads via the also-thread-local ThreadDefs.
 */
public class CoreDefs extends FieldDefs {
    static final ThreadLocal<CoreDefs> defs = ThreadLocal.withInitial(() -> new UnsetCoreDefs());

    // only affects nv2 and stdout output not native formats
    private final static int DEFAULT_DOUBLE_PRECISION = 4;

    private final AtomicBoolean initialized = new AtomicBoolean(false); // only happens once
    private final AtomicInteger numSourceThreads = new AtomicInteger();
    private final AtomicInteger numSinkThreads = new AtomicInteger();
    private final AtomicInteger numSortBuckets = new AtomicInteger();
    private final AtomicInteger numSortThreads = new AtomicInteger();
    private final AtomicInteger doublePrintPrecision = new AtomicInteger();
    private final AtomicInteger reportRefreshSecs = new AtomicInteger();
    // AtomicReference<FieldNameRules> fieldNameRules defined in base class FieldDefs
    private final AtomicReference<Display.DisplayType> displayType = new AtomicReference<Display.DisplayType>();

    private Namespace propertiesNamespace = new Namespace();

    /*
       The following are System Properties that have scope within the namespace of job.
       They can be prepended to an expression like this:
           unix> djk numSinkThreads=1\; numSourceThreads=2\; [ hello:world ]  # note the backslashes
       They can also be set via Environment variables before execution at the command line like this:
           unix> export numSourceThreads=1; djk [ hello:world ]

       When InnerKnife starts up, it checks for Environment variables, if they exist they are used
       otherwise the default value is set.  These values will be overridden by properties prepended
       to the command.
    */
    public static final String NUM_SINK_THREADS = "numSinkThreads";
    public static final String NUM_SOURCE_THREADS = "numSourceThreads";
    public static final String NUM_SORT_THREADS = "numSortThreads";
    public static final String NUM_SORT_BUCKETS = "numSortBuckets";
    public static final String DOUBLE_PRINT_PRECISION = "doublePrintPrecision";
    public static final String FIELD_NAME_RULES = "fieldNameRules";
    private final static String REPORT_REFRESH_SECS = "reportRefreshSecs";
    public final static String DISPLAY_TYPE = "displayType";

    public static class UnsetCoreDefs extends CoreDefs {
        @Override
        public void initProperties() { /* so this dummy doesn't get initialized */ }
    }

    public static CoreDefs get() throws IOException {
        CoreDefs cds = defs.get();
        if (cds instanceof UnsetCoreDefs) {
            throw new DJKInitializationException(Type.CORE_DEFS);
        }

        return cds;
    }

    static boolean isInitialized() {
        CoreDefs cds = defs.get();
        return (!(cds instanceof UnsetCoreDefs));
    }

    static void initialize() {
        defs.set(new CoreDefs());
    }

    static void deinitialize() {
        defs.set(new UnsetCoreDefs());
    }

    /**
     *
     */
    CoreDefs() {
        initProperties();
    }

    /**
     * initial the system properties.  Only can happen once.
     */
    public void initProperties() {
        if (initialized.get()) return;
        setCoreIntProperty(NUM_SINK_THREADS, Runtime.getRuntime().availableProcessors(), true);
        setCoreIntProperty(NUM_SOURCE_THREADS, Math.max(numSinkThreads.get() / 2, 1), true);
        setCoreIntProperty(NUM_SORT_BUCKETS, 80, true);
        setCoreIntProperty(NUM_SORT_THREADS, numSourceThreads.get(), true);
        setCoreIntProperty(DOUBLE_PRINT_PRECISION, DEFAULT_DOUBLE_PRECISION, true);
        setCoreIntProperty(REPORT_REFRESH_SECS, 3, true);
        setCoreProperty(FIELD_NAME_RULES, FieldNameRules.ENFORCE_REGEX.toString(), true);
        setCoreProperty(DISPLAY_TYPE, Display.DisplayType.DEFAULT.toString(), true);
        initialized.set(true);
    }

    public static boolean isCoreProperty(String propertyName) {
        switch (propertyName) {
            case NUM_SINK_THREADS:
            case NUM_SOURCE_THREADS:
            case NUM_SORT_BUCKETS:
            case NUM_SORT_THREADS:
            case DOUBLE_PRINT_PRECISION:
            case REPORT_REFRESH_SECS:
            case FIELD_NAME_RULES:
            case DISPLAY_TYPE:
                return true;

            default:
                return false;
        }
    }

    public boolean setCoreIntProperty(String propertyName, int value) {
        return setCoreIntProperty(propertyName, value, false);
    }


    /**
     * set core properties
     * <p>
     * if prioritizeExternal is true, then this hierarchy is observed: SystemProperty >> EnvironmentVariable >> value
     * else use value
     *
     * @param propertyName
     * @param value
     * @param prioritizeExternal
     * @return
     */
    public boolean setCoreIntProperty(String propertyName, int value, boolean prioritizeExternal) {
        String valueAsString = Integer.toString(value);
        return setCoreProperty(propertyName, valueAsString, prioritizeExternal);
    }

    /**
     * set core properties
     *
     * @param propertyName
     * @param valueAsString
     * @return true if property set
     */
    public boolean setCoreProperty(String propertyName, String valueAsString) {
        return setCoreProperty(propertyName, valueAsString, false);
    }

    /**
     * set core properties
     * <p>
     * if prioritizeExternal is true, then this hierarchy is observed: SystemProperty >> EnvironmentVariable >> valueAsString
     * else use valueAsString
     * <p>
     * afterwards SystemProperty(propertyName) reflects selected value
     *
     * @param propertyName
     * @param valueAsString
     * @param prioritizeExternal
     * @return true if property set
     */
    public boolean setCoreProperty(String propertyName, String valueAsString, boolean prioritizeExternal) {
        if (prioritizeExternal) {
            String externalValue = System.getProperty(propertyName);
            if (externalValue == null) {
                externalValue = System.getenv(propertyName);
            }

            valueAsString = externalValue != null ? externalValue : valueAsString;
        }

        switch (propertyName) {
            case NUM_SINK_THREADS:
                numSinkThreads.set(Integer.parseInt(valueAsString));
                break;

            case NUM_SOURCE_THREADS:
                numSourceThreads.set(Integer.parseInt(valueAsString));
                break;

            case NUM_SORT_BUCKETS:
                numSortBuckets.set(Integer.parseInt(valueAsString));
                break;

            case NUM_SORT_THREADS:
                numSortThreads.set(Integer.parseInt(valueAsString));
                break;

            case DOUBLE_PRINT_PRECISION:
                doublePrintPrecision.set(Integer.parseInt(valueAsString));
                break;

            case REPORT_REFRESH_SECS:
                reportRefreshSecs.set(Integer.parseInt(valueAsString));
                break;

            case FIELD_NAME_RULES:
                fieldNameRules.set(FieldNameRules.getType(valueAsString));
                break;

            case DISPLAY_TYPE:
                displayType.set(Display.DisplayType.getDisplayType(valueAsString));
                break;

            default:
                return false;
        }

        return true;
    }

    // only getters are provided, setting must happen using above methods

    /**
     * @return
     */
    public int getNumSinkThreads() {
        return numSinkThreads.get();
    }

    /**
     * @return
     */
    public int getNumSourceThreads() {
        return numSourceThreads.get();
    }

    /**
     * @return
     */
    public int getNumSortThreads() {
        return numSortThreads.get();
    }

    /**
     * @return
     */
    public int getNumSortBuckets() {
        return numSortBuckets.get();
    }

    /**
     * @return
     */
    public int getReportRefreshSecs() {
        return reportRefreshSecs.get();
    }

    /**
     * @return
     */
    public int getDoublePrintPrecision() {
        return doublePrintPrecision.get();
    }

    /**
     * @return
     */
    public Display.DisplayType getDisplayType() {
        return displayType.get();
    }

    /**
     * returns the value for this property within the appropriate namespace.  If no property found,
     * an attempt will be made to find it in the global namespace.
     *
     * @param name of the property (without namespace)
     * @return the property
     * @throws SyntaxError
     */
    public String getProperty(String name) throws SyntaxError {
        return KnifeProperties.getProperty(getPropertiesNamespace(), name);
    }

    public Namespace getPropertiesNamespace() {
        return propertiesNamespace;
    }

    public void setPropertiesNamespace(Namespace namespace) {
        propertiesNamespace = namespace;
    }
}
