package com.amazon.djk.pipe;

import com.amazon.djk.core.MinimalRecordSource;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.format.FormatException;
import com.amazon.djk.format.NV2FormatParser;
import com.amazon.djk.format.PushbackLineReader;
import com.amazon.djk.manual.Description;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

@ReportFormats(headerFormat="http://<endpoint>%s?batchSize=%d",
	lineFormats={"posts=%,d waiting=%d waitingRecs=%d"})
public class RestPipe extends RecordPipe {
    private static final String DEFAULT_BATCH_SIZE = "30";
    public static final String BATCH_SIZE = "batchSize";
    public static final String ENDPOINT = "ENDPOINT";
    private final OpArgs args;
    private BatchResponseSource batchResponse = null;

    @ScalarProgress(name="batchSize", aggregate=AggType.NONE)
    private final long batchSize;

    @ScalarProgress(name="endpoint")
    private final String endpoint;

    @ScalarProgress(name="waiting")
    private volatile long waitingBatches = 0;
    
    @ScalarProgress(name="waitingRecs")
    private volatile long waitingRecs = 0;
    
    @ScalarProgress(name="posts")
    private volatile long numPosts = 0;
    
    private boolean superExhausted = false;
        
	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public RestPipe(OpArgs args) throws IOException {
	    this(null, args);
	}

	/**
	 * 
	 * @param root
	 * @param args
	 * @throws IOException
	 */
    public RestPipe(RestPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        endpoint = (String)args.getArg(ENDPOINT);
        batchSize = (Long)args.getParam(BATCH_SIZE);
    }
    
    @Override
    public Object replicate() throws IOException {
    	return new RestPipe(this, args);
    }
    
    private static HttpURLConnection getConnection(String endpoint) throws IOException {
        URL url = new URL(String.format("http://%s", endpoint));
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty( "Content-Type", "text/plain"); 
        conn.setRequestProperty( "charset", "utf-8");
        conn.setRequestProperty("Connection", "keep-alive");
        
        return conn;
    }
    
    /**
     * there is a bug in this pipe where when super.next() return null,
     * it calls super.next() again. This method protects the upstream
     * components from an illegal next().
     * 
     * @return
     * @throws IOException 
     */
    private Record mySuperNext() throws IOException {
        if (superExhausted) return null;
        Record rec = super.next();
        if (rec != null) return rec;
        superExhausted = true;
        return null;
    }

    /**                                                                                                                                                                     
     * Collect up a batch of records of max batchSize.  If there are no records                                                                                             
     * return null. Otherwise return a source of the response to that batch.                                                                                                
     *                                                                                                                                                                      
     * @param batchSize                                                                                                                                                     
     * @return                                                                                                                                                              
     * @throws IOException                                                                                                                                                  
     */
    private BatchResponseSource postBatch(int batchSize) throws IOException {
        StringBuilder sb = new StringBuilder();
        waitingRecs = 0;
        for (int i = 0; i < batchSize; i++) {
            Record rec = mySuperNext();
            if (rec == null) break;

            sb.append(rec);
            sb.append("#\n");
            waitingRecs++;
        }

        if (sb.length() == 0) return null;
        return new BatchResponseSource(sb.toString(), endpoint);
    }

    
    /**
     * batches records for a single post 
     */
    private class BatchResponseSource extends MinimalRecordSource {
        private final NV2FormatParser parser;
        private final PushbackLineReader plr; 
        private final BufferedReader br;
        private final HttpURLConnection conn;
        
        public BatchResponseSource(String payload, String endpoint) throws IOException {
            conn = getConnection(endpoint);
            conn.setRequestProperty( "Content-Length", Integer.toString( payload.length() ));
            
            try( DataOutputStream wr = new DataOutputStream( conn.getOutputStream())) {
                wr.write( payload.getBytes("UTF-8") );
                wr.flush();
                wr.close();
            }

            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new IOException("Failed : HTTP error code : "
                    + conn.getResponseCode());
            }

            br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));
            
            parser = new NV2FormatParser();
            plr = new PushbackLineReader(br);
        }
        
        @Override
        public Record next() throws IOException {
            if (parser == null) return null;
            Record rec = null;

            try {
				rec = parser.next(plr);
			} catch (FormatException e) {
				throw new IOException(e.getMessage());
			}
			
            if (rec != null) return rec;

            plr.close();
            br.close();
            conn.disconnect();
            return null;
        }
    }
    
    @Override
    public Record next() throws IOException {
        Record rec = null;
        
        while (rec == null) {
            if (batchResponse == null) {
            	waitingBatches++;
            	batchResponse = postBatch((int)batchSize);
                waitingBatches--;
                
                if (batchResponse == null) {
                    return null;
                }
                
                numPosts++;
                rec = batchResponse.next();
                if (rec == null) {
                    batchResponse = null;
                }
            }
            
            else {
                rec = batchResponse.next();
                if (rec == null) {
                    batchResponse = null;
                }
            }
        }
        
        return rec;
    }
    
   @Description(text={"POSTs records to the rest endpoint."})
   @Arg(name= ENDPOINT, gloss="host:port/resource", type=ArgType.STRING)
   @Param(name= BATCH_SIZE, gloss="The number of records batched per post request.", type=ArgType.LONG, defaultValue = DEFAULT_BATCH_SIZE)
   public static class Op extends PipeOperator {
       public Op() {
           super("rest://ENDPOINT");
       }
           
       @Override
       public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
           return new RestPipe(args).addSource(operands.pop());
       }
   }
}
