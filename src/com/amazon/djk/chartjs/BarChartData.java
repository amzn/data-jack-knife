package com.amazon.djk.chartjs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.record.Record;

public class BarChartData extends ChartData {
	// in order they were received
	private final List<String> xValuesList = new ArrayList<>();

	// unique values
	private final Set<String> xValuesSet = new HashSet<>();

	// make sure we don't repeat
	private final Set<String> xDataSetKeys = new HashSet<>();

	private final Set<String> dataSetNames = new HashSet<>();
	
	// key = dataSetName:xValue
	private final Map<String,Object> yValues = new HashMap<>();

	public BarChartData(OpArgs args) throws IOException {
		super(args);
	}
	
	private void addYvalue(Object yValue, String dataSetKey) throws SyntaxError {
		yValues.put(dataSetKey, yValue); 
	}
	
	private void addXvalue(String xValue, String dataSetKey) throws SyntaxError {
		if (!xDataSetKeys.add(dataSetKey)) { // if already has it
			throw new SyntaxError("xValues must be uniq per data set");
		}
		
		if (xValuesSet.add(xValue)) { // if change
			xValuesList.add(xValue);	
		}
	}
	
	public void add(Record point) throws IOException {
		String dataSetName = point.getFirstAsString(dataSetField);
		String xValue = getXStringValue(point);
		Object yValue = getYvalue(point);
		if (dataSetName == null) dataSetName = "all";
		
		String dataSetKey = String.format("%s:%s", dataSetName, xValue);
		addXvalue(xValue, dataSetKey);
		addYvalue(yValue, dataSetKey);
		dataSetNames.add(dataSetName);

	}

	/**
	 *	 {
	 *      labels: ['M', 'T', 'W', 'T', 'F', 'S', 'S'],
	 *	    datasets: [
	 *      {
	 *	      label: 'apples',
	 *	      data: [12, 19, 3, 17, 6, 3, 7],
	 *	      backgroundColor: "rgba(153,255,51,0.4)"
	 *	    }, 
	 *      {
	 *	      label: 'oranges',
	 *	      data: [2, 29, 5, 5, 2, 3, 10],
	 *	      backgroundColor: "rgba(255,153,0,0.4)"
	 *	    },
	 *     ]
	 *	  }
	 */
	public void appendData(StringBuilder sb) {
		sb.append("{\n"); // opening data

		sb.append("labels: [");
		for (String xLabel : xValuesList) {
			sb.append("'");
			sb.append(xLabel);
			sb.append("',");
		}
		sb.append("],\n");
		
		sb.append("datasets: [");
		int dsetNo = 0;
		for (String dset : dataSetNames) {
			addDataSet(sb, dset, getColor(dsetNo++));
		}
		sb.append("]"); // end datasets
		
		sb.append("},\n"); // closing data
	}
	
	/**
	 * 
	 * @param sb
	 * @param dataSetName
	 * @param backgroundColor
	 */
	private void addDataSet(StringBuilder sb, String dataSetName, Color backgroundColor) {
		sb.append("{\n");
		sb.append(String.format("label: '%s',\n", dataSetName));
		sb.append(String.format("backgroundColor: '%s',\n", backgroundColor));
		
		sb.append("data: [");
		for (String xValue : xValuesList) {
			String dataSetKey = String.format("%s:%s", dataSetName, xValue);
			Object yValue = yValues.get(dataSetKey);
			sb.append(yValue != null ?
					(yValue instanceof Long ? 
						String.format("%d,", yValue) : 
						String.format("%2.2f,", yValue)) : "0,");
		}
		
		sb.append("],\n");
		sb.append("},");
	}
}
