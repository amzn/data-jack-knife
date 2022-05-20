package com.amazon.djk.chartjs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.record.Record;

/**
 * http://www.chartjs.org/docs/latest/getting-started/
 * https://stackoverflow.com/questions/44661671/chart-js-scatter-chart-displaying-label-specific-to-point-in-tooltip
 * http://www.chartjs.org/docs/latest/configuration/tooltip.html
 * https://github.com/chartjs/Chart.js/issues/1178 -->
 */
	
public class PointChartData extends ChartData {
	private static class Point {
		public final Object xValue;
		public final Object yValue;
		public final String label;
		public final String href;
		
		public Point(Object xValue, Object yValue, String label, String href) {
			this.xValue = xValue;
			this.yValue = yValue;
			this.label = label;
			this.href = href;
		}
	}

	private final List<List<Point>> pointsList = new ArrayList<>();
	private final Map<String,Integer> dataSetNos = new HashMap<>();
	
	public PointChartData(OpArgs args) throws IOException {
		super(args);
	}
	
	private int getDataSetNo(String dataSet) {
		Integer no = dataSetNos.get(dataSet);
		if (no != null) return no;

		no = dataSetNos.size();
		dataSetNos.put(dataSet, no);
		pointsList.add(new ArrayList<>());
		
		return no;
	}

	@Override
	public void add(Record point) throws IOException {
		String dataSetName = point.getFirstAsString(dataSetField);
		Object xValue = getXvalue(point);
		Object yValue = getYvalue(point);
		String label = getOtherFieldsAsLabel(point);
		String href = getHref(point);
		
		if (dataSetName == null) dataSetName = "all";
		int dataSetNo = getDataSetNo(dataSetName);
		List<Point> points = pointsList.get(dataSetNo);
		
		points.add(new Point(xValue, yValue, label, href));
	}
	
	public void appendData(StringBuilder sb) {
		sb.append("{\n"); // opening data

		sb.append("datasets: [");
		for (String dset : dataSetNos.keySet()) {
			int dsetNo = dataSetNos.get(dset);
			addDataSet(sb, dset, getColor(dsetNo));
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
		sb.append("radius: 2,\n");
		sb.append("pointHoverRadius: 5,\n");
		sb.append(String.format("label: '%s',\n", dataSetName));
		sb.append(String.format("backgroundColor: '%s',\n", backgroundColor));
		
		int dataSetNo = getDataSetNo(dataSetName);
		List<Point> points = pointsList.get(dataSetNo);
		
		sb.append("data: ["); // data begin
		for (Point point : points) {
			sb.append("{\n");
			sb.append(point.xValue instanceof Long ?
					String.format("x: %d,\n", point.xValue) :
					String.format("x: %2.2f,\n", point.xValue));
			sb.append(point.yValue instanceof Long ?
					String.format("y: %d,\n", point.yValue) :
					String.format("y: %2.2f,\n", point.yValue));
			
			if (point.label != null) {
				sb.append(String.format("label:'%s',\n", point.label));
			}
			if (point.href != null) {
				sb.append(String.format("href:'%s',\n", point.href));
			}
			sb.append("},\n");			
		}
		
		sb.append("],\n"); // end data [
		sb.append("},");  // end dataset
	}
}
