package com.amazon.djk.chartjs;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.amazon.djk.record.Record;

public interface Chart {
	void addCanvasDiv(StringBuilder sb);
	void addScriptContent(StringBuilder sb);
	void add(Record point) throws IOException;
	
	public static String getCanvasId(int chartId) {
		return String.format("canvasId%d", chartId);
	}
	
	public static void addCanvasDiv(int chartId, StringBuilder sb) {
		sb.append("<div style=\"width:90%\">\n");
		sb.append(String.format(" <canvas id=\"canvasId%d\"></canvas>\n", chartId));
		sb.append("</div>\n");
	}
	
	public static void getAsPage(Chart chart, StringBuilder sb) {
		sb.append("<html>\n");
		sb.append("<script src=\"https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.7.0/Chart.min.js\"></script>\n");
		sb.append("<body>\n");
		chart.addCanvasDiv(sb);
		sb.append("<script>\n");

		chart.addScriptContent(sb);
		
		sb.append("</script>\n");
		sb.append("</body>\n");
		sb.append("</html>\n");
	}
	
	public static void writeFile(File file, Chart chart) throws IOException {
		StringBuilder sb = new StringBuilder();
		getAsPage(chart, sb);
		FileUtils.writeStringToFile(file, sb.toString(), "UTF-8");
	}
}
