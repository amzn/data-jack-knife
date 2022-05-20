package com.amazon.djk.chartjs;

import java.io.IOException;
import java.util.Random;

import com.amazon.djk.chartjs.Scales.Axis;
import com.amazon.djk.chartjs.Scales.Scale;
import com.amazon.djk.chartjs.Scales.Axis.XorY;
import com.amazon.djk.chartjs.Scales.ChartKind;
import com.amazon.djk.expression.OpArgs;

public class BarChart extends BarChartData implements Chart {
	private final int chartId;

	public BarChart(OpArgs args) throws IOException {
		super(args);
		this.chartId = Math.abs(new Random().nextInt());
	}
	
	/*
	public BarChart(String title, String dataSetField, String xField, String yField) throws SyntaxError {
		this(title, new Field(dataSetField), new Field(xField), new Field(yField));
	}*/

	public void addCanvasDiv(StringBuilder sb) {
		Chart.addCanvasDiv(chartId, sb);
	}
	
	public void addScriptContent(StringBuilder sb) {
		sb.append(String.format("var contextId%d = document.getElementById('canvasId%d').getContext('2d');\n", chartId, chartId));
		sb.append(String.format("var chartId%s = new Chart(contextId%d, {\n", chartId, chartId));
	
		sb.append("type: 'bar',\n");
		sb.append("data:\n");
		appendData(sb);
		sb.append("options: {\n");

		
		
		sb.append(getTitle());
        sb.append(Scales.get(ChartKind.BAR,
      		   new Axis(XorY.xAxes, xField.getName(), getXmax(), Scale.BAR), 
     		   new Axis(XorY.yAxes, yField.getName(), getYmax(), isYLog ? Scale.LOGARITHMIC : Scale.LINEAR)));
        
		sb.append("}\n"); // options
		sb.append("});\n");
	}
}
