package com.amazon.djk.sort;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ProgressData;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import com.amazon.djk.report.ScalarResolver.AggType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@ReportFormats(headerFormat="<sortSpecs>%s", lineFormats={
		"state=%s numRecs=%,d memMBytes=%1.1f",
		"<uniqBy>%s" // no line shown if uniqBy=null
})
public class SortPipe extends RecordPipe {
    private static final String UNIQ_BY = "uniqBy";
    private static final String UNIQ_COUNT = "uniqCount";
    private static final String DEFAULT_UNIQ_COUNT = "1";

    @ScalarProgress(name="sortSpecs")
	private final String displaySortSpecs;
    private List<Record> recordList = new ArrayList<>();
    private enum SortState {INPUT, OUTPUT};
    
    @ScalarProgress(name="state", aggregate=AggType.NONE)
    private SortState state = SortState.INPUT;
    
    @ScalarProgress(name="numRecs", aggregate=AggType.NONE)
    private int numRecs = 0;
    
    @ScalarProgress(name=UNIQ_BY)
    private String uniqByDisplay = null;
    
    @ScalarProgress(name="memMBytes", aggregate=AggType.NONE, multiplier=0.000001)
    private long byteSize = 0;

    private final SortSpec[] sortSpecs;
    private final RecordSorter sorter;
    
    private final SortUniquePipe uniq;
    
    private int[] sortedOrds;
    private int outputIdx = 0;
    
    public SortPipe(SortSpec[] sortSpecs, SortUniquePipe uniq) throws IOException {
        super(null);
        this.sortSpecs = sortSpecs;
        sorter = new RecordSorter(sortSpecs);
        this.uniq = uniq; 
        
        StringBuilder sb = new StringBuilder();
        for (SortSpec spec : sortSpecs) {
            if (sb.length() != 0) sb.append(','); 
            sb.append(spec);
        }
        this.displaySortSpecs = sb.toString();
    }
    
    public SortPipe(SortSpec[] specs) throws IOException {
    	this(specs, null);
    }
    
    @Override
	public ProgressData getProgressData() {
    	if (uniq != null) {
    		uniqByDisplay = String.format("uniqBy=%s dupsEliminated=%,d", uniq.toString(), uniq.getNumDups());
    	}
    	
    	return new ProgressData(this);
    }
    
    @Override
    public Object subReplicate () throws IOException {
        return new SortPipe(sortSpecs);
    }

    @Override
    public Record next() throws IOException {
        switch (state) {
        case INPUT: // for v1 we simply exhaust the input
            while (true) {
                Record rec = super.next();
                if (rec == null) break;
                
                recordList.add(rec.getCopy());
            	numRecs++;
            	byteSize += rec.size();
            }

            sortedOrds = sorter.sort(recordList);
            outputIdx = 0;
            state = SortState.OUTPUT;
            // fall through
            
        case OUTPUT:
            if (outputIdx >= recordList.size()) return null;
            return recordList.get(sortedOrds[outputIdx++]);
            
        default:
            return null;
        }
    }
    
    @Override
    public boolean reset() {
        recordList.clear();
        state = SortState.INPUT; // in case reused.
        byteSize = 0;
        numRecs = 0;
    	return true;
    }
    
    public static PipeOperator getOperator() {
    	return new Op();
    }
    
    @Description(text={"sorts the incoming records by the sort SPECS"})
    @Gloss(entry="SPEC", def="{+-}.TYPE.FIELD, e.g. +d.price for 'ascending double price'")
    @Gloss(entry="FIELD", def="name of the field to sort by")
    @Gloss(entry="TYPE", def="d | l | s for double, long or string respectively")

    @Example(expr="[ name:zeek name:billy name:amy ] sort:-s.name", type=ExampleType.EXECUTABLE)
    @Example(expr="[ color:red,cost:4 color:red,cost:8 color:red,cost:2 color:blue,cost:1 ] sort:+s.color,-d.cost'?uniqBy=color'", type=ExampleType.EXECUTABLE)
    @Example(expr="[ c:A,v:1 c:X,v:8 c:A,v:2 c:A,v:4 c:X,v:10 c:X,v:0 c:B,v:0 ] sort:+s.c,-d.v'?uniqBy=c&uniqCount=2'", type=ExampleType.EXECUTABLE)
    @Arg(name="SPECS", gloss="comma separated list of SPEC", type=ArgType.STRINGS)
    @Param(name=UNIQ_BY, gloss="comma separated list of fields. Uniques sorted list by fields", type=ArgType.FIELDS)
    @Param(name=UNIQ_COUNT, gloss="Number of instances of 'uniqBy' to be kept.", type=ArgType.LONG, defaultValue = DEFAULT_UNIQ_COUNT)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("sort:SPECS");
    	}
         
        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		String[] specStrings = (String[])args.getArg("SPECS");
    		SortSpec[] sortSpecs = new SortSpec[specStrings.length];
    		for (int i = 0; i < specStrings.length; i++) {
    		    sortSpecs[i] = new SortSpec(specStrings[i]);
    		}
    		
    		Fields uniqByFields = (Fields)args.getParam(UNIQ_BY);
    		long uniqCount = (Long)args.getParam(UNIQ_COUNT);
    		
    		SortUniquePipe uniq = null;
    		if (uniqByFields != null) {
    			uniq = new SortUniquePipe(uniqByFields, uniqCount);
    	        uniq.suppressReport(true);
    		}
    		
    		// add uniq to the sort, just for reporter info
    		RecordPipe sorter = new SortPipe(sortSpecs, uniq);    		
    		sorter.addSource(operands.pop());

    		if (uniq != null) {
    			uniq.addSource(sorter); // add sorter to uniq for the data flow
    			return uniq;
    		}

    		return sorter;
    	}
    }
}
