package com.amazon.djk.source;

import java.io.IOException;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.core.MinimalRecordSource;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.core.Splittable;
import com.amazon.djk.keyed.KeyedSource;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.processor.FieldDefs;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.ThreadDefs;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.expression.SyntaxError;

public class EmptyKeyedSource extends KeyedSource {
	public final static String FORMAT = "empty";
	private final Fields fields;
	
	public EmptyKeyedSource(OpArgs args, Fields keyFields) throws IOException {
		super(args, keyFields);
		fields = keyFields;
	}

	@Override
    public Object replicateKeyed() throws IOException {
        return new EmptyKeyedSource(args, fields);
    }
    
    @Override
    public Record getValue(Record keyRecord) throws IOException {
    	return null;
    }

    @ReportFormats(headerFormat="<header>%s")
    public static class EmptySource extends MinimalRecordSource implements Splittable {
    	private int numSplits = 1;
    	 @ScalarProgress(name="header")
    	private final String reportHeader;
    	
    	public EmptySource(String reportHeader) {
    		this.reportHeader = reportHeader;
    	}
    	
    	public EmptySource() {
    		reportHeader = "";
    	}
	
    	@Override
    	public Object split() throws IOException {
    		if (numSplits < ThreadDefs.get().getNumSinkThreads()) {
    			numSplits++;
    			return new EmptySource();
    		} 
		
    		else return null;
    	}
	}
	
	/**
     * 
     * @return
     */
	
    @Description(text={"An empty keyed source of records.  Can be used in non-keyed contexts as well."})
    @Example(expr="[ hello:world ] empty join:left", type=ExampleType.EXECUTABLE)
    @Example(expr="empty devnull", type=ExampleType.EXECUTABLE_GRAPHED)
    public static class Op extends SourceOperator {
    	public Op() {
			super(FORMAT, Type.NAME);
		}

		@Override
    	public RecordSource getSource(OpArgs args) throws IOException, SyntaxError {
    		return new EmptySource();
    	}
		
		@Override
        public KeyedSource getKeyedSource(OpArgs accessArgs) throws IOException {
			// non-matchable field
			Fields fields = new Fields(FieldDefs.DELETED_FIELD);
            return new EmptyKeyedSource(accessArgs, fields);
        }
    }
}
