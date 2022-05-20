package com.amazon.djk.sink;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.file.FileSystem;
import com.amazon.djk.file.FormatArgs;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.RecordFIFO;
import com.amazon.djk.record.RecordIO;
import com.amazon.djk.record.ThreadDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSinkHelper {
    private static Logger LOG = LoggerFactory.getLogger(FileSinkHelper.class);
    private final static String FILE_NAME = "records";

    private final String absolutePath;
    private final String format;
    private final boolean asFile;
    private final boolean useGzip;
    private final boolean overwrite;

    private int numFiles = 0;
    private final File sinkPath;

    /**
     * constructor for Sink (e.g. MapDB)
     *
     * @param args
     * @throws IOException
     */
    public FileSinkHelper(String pathString, String format, boolean overwrite) throws IOException {
        this(pathString, format, overwrite, false, false);
    }

    public FileSinkHelper(String absolutePath, String format, boolean overwrite, boolean asFile, boolean useGzip) throws IOException {
        this.absolutePath = absolutePath;
        this.format = format;
        this.asFile = asFile;
        this.useGzip = useGzip;
        this.overwrite = overwrite;

        this.sinkPath = new File(absolutePath());
        prepareDirectory();
    }

    public String absolutePath() {
        return absolutePath;
    }

    public String format() {
        return format;
    }

    public boolean useGzip() {
        return useGzip;
    }

    public boolean overwrite() {
        return overwrite;
    }

    public boolean asFile() {
        return asFile;
    }

    /**
     * @return
     * @throws IOException
     */
    public File getSinkPath() {
        return sinkPath;
    }

    public File getDataFile() {
        return asFile() ? new File(absolutePath()) : getNextFile();
    }

    /**
     * prepare the directory for sinking.
     *
     * @throws IOException
     */
    private void prepareDirectory() throws IOException {
        if (asFile) return;

        if (!sinkPath.exists()) {
            if (!sinkPath.mkdirs()) {
                throw new SyntaxError("unable to make directory:" + sinkPath);
            }

            return;
        }

        if (!sinkPath.isDirectory()) {
            throw new SyntaxError("file exists with given directory path.");
        }

        // we're a directory
        File[] files = sinkPath.listFiles();
        if (files.length != 0 && !overwrite()) {
            throw new SyntaxError("directory '" + sinkPath + "' is not empty");
        }

        cleanRecursive(sinkPath);
    }

    private void cleanRecursive(File dir) throws IOException {
        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                cleanRecursive(file);
            } else {
                if (!file.delete()) {
                    throw new IOException("could not overwrite existing directory");
                }
            }
        }
    }

    /**
     * @return
     */
    public File getNextFile() {
        String suffix = useGzip() ? String.format("%s.gz", format()) : String.format("%s", format());
        String name = FileSystem.getNumberedFileName(FILE_NAME, numFiles++, suffix);
        return new File(sinkPath, name);
    }

    /**
     * persists the report record to a file in the directory in a quasi-native format.
     * (allows multi-line strings to be stored)
     *
     * @throws IOException
     */
    public static void persistOriginReport(Record originReport, File outputDir) throws IOException {
        // quasi-native means we write the field list first in the same file.
        List<String> fields = ThreadDefs.get().getFieldList();

        File file = new File(outputDir, FormatArgs.REPORT_NAT_FILE);
        FileOutputStream fos = new FileOutputStream(file);
        DataOutputStream outstream = new DataOutputStream(fos);

        // write fields (avoid writing as single string because could be > 64K)
        outstream.writeInt(fields.size());
        for (String field : fields) {
            outstream.writeUTF(field);
        }

        // write record
        RecordFIFO fifo = new RecordFIFO();
        fifo.add(originReport);
        RecordIO.write(outstream, fifo);

        outstream.close();
    }

    /**
     * retrieves the origin report persisted with the above static method
     *
     * @param sourceDir
     * @return
     * @throws IOException
     */
    public static Record getOriginReport(File sourceDir) {
        File file = new File(sourceDir, FormatArgs.REPORT_NAT_FILE);
        try (FileInputStream fis = new FileInputStream(file)) {
            Record out = getOriginReport(fis);
            return out;
        } catch (IOException e) {
            LOG.warn(String.format("could not read origin file '%s'", FormatArgs.REPORT_NAT_FILE), e);
            return null;
        }
    }

    public static Record getOriginReport(InputStream instream) throws IOException {
        DataInputStream datastream = new DataInputStream(instream);
        RecordFIFO fifo = new RecordFIFO();

        int numFields = datastream.readInt();
        String[] fields = new String[numFields];
        for (int i = 0; i < numFields; i++) {
        	fields[i] = datastream.readUTF();
		}

        RecordIO recordIO = new RecordIO(fields);
        boolean notDone = recordIO.fill(datastream, fifo);

        Record out = fifo.next(); // should be the only record in the fifo, see above
        if (fifo.next() != null) {
            throw new IOException("too many records persisted in origin report");
        }

        notDone = recordIO.fill(datastream, fifo);
        if (notDone) {
            throw new IOException("too many records persisted in origin report");
        }

        return out;
    }
}
