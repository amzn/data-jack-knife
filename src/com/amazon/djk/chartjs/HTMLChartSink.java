package com.amazon.djk.chartjs;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
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
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats2;
import com.amazon.djk.report.ScalarProgress;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.amazon.djk.chartjs.ChartData.DATASET_FIELD;
import static com.amazon.djk.chartjs.ChartData.DEFAULT_TITLE;
import static com.amazon.djk.chartjs.ChartData.HREF_FIELD;
import static com.amazon.djk.chartjs.ChartData.LOG_PARAM;
import static com.amazon.djk.chartjs.ChartData.TITLE_PARAM;
import static com.amazon.djk.chartjs.ChartData.X_FIELD;
import static com.amazon.djk.chartjs.ChartData.Y_FIELD;

/**
 * 
 * 
 */
@ReportFormats2(headerFormat="file://<file>%s")
public class HTMLChartSink extends RecordSink {
    private static final String TYPE_PARAM = "type";
    @ScalarProgress(name="file")
	private final String file;
	private final Chart chart;
	
	public HTMLChartSink(OpArgs args) throws IOException {
	    super(null);
	    file = (String)args.getArg("FILE");
	    String type = (String)args.getParam(TYPE_PARAM);
	    
    	Chart ch = null;
    	switch (type) {
    	case "bar":
    		ch = new BarChart(args);
    		break;
    		
    	case "point":
    		ch = new PointChart(args);
    		break;
    		
    		default:
    			throw new SyntaxError("unsupported chart type");
    	}
    	
    	chart = ch;
	}
	
    @Override
    public void drain(AtomicBoolean forceDone) throws IOException {
    	super.drain(forceDone);
        while (!forceDone.get()) {
            Record rec = next();
            if (rec == null) break;
            reportSunkRecord(1);
            chart.add(rec);
        }
        
        File out = new File(file);
        Chart.writeFile(out, chart);
    }
    
    @Description(text={"Creates chart.js html based on input record(s).  A label is created for point from all fields of input records excluding the ones below."})
    @Arg(name = "FILE", gloss = "the location of the html file.", type=ArgType.STRING)
    @Param(name = X_FIELD, gloss = "Field name of the x-values.", type=ArgType.FIELD, defaultValue = X_FIELD)
    @Param(name = Y_FIELD, gloss = "Field name of the y-values.", type=ArgType.FIELD, defaultValue = Y_FIELD)
    @Param(name = DATASET_FIELD, gloss = "Name of the field specifying the data set name.  If non-existent, a single data set is assumed.", type=ArgType.FIELD, defaultValue=DATASET_FIELD)
    @Param(name = TYPE_PARAM, gloss = "The chart type, either 'bar' or 'point'.", type=ArgType.STRING, defaultValue="bar")
    @Param(name = LOG_PARAM, gloss = "comma separated list of the axis that should be logarithmic.", type=ArgType.STRINGS, eg="x,y", defaultValue = "")
    @Param(name = TITLE_PARAM, gloss = "The chart title.", type=ArgType.STRING, defaultValue = DEFAULT_TITLE)
    @Param(name = HREF_FIELD, gloss = "For point charts, field name of an href to make the point clickable.", type=ArgType.FIELD, defaultValue=HREF_FIELD)
    @Example(expr="djk [ x:10,y:20,href:'{\"http://www.amazon.com\";}' ] chartjs:/tmp/chart.js'?type=point'", type=ExampleType.DISPLAY_ONLY)
    public static class Op extends PipeOperator {
    	public Op() {
    		super("chartjs:FILE");
    	}
    
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		return new HTMLChartSink(args).addSource(operands.pop());
    	}
    }
}
