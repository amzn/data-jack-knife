package com.amazon.djk.chartjs;

import java.io.IOException;
import java.util.Random;

import com.amazon.djk.chartjs.Scales.Axis;
import com.amazon.djk.chartjs.Scales.Axis.XorY;
import com.amazon.djk.chartjs.Scales.ChartKind;
import com.amazon.djk.chartjs.Scales.Scale;
import com.amazon.djk.expression.OpArgs;

public class PointChart extends PointChartData implements Chart {
	private final int chartId;

	public PointChart(OpArgs args) throws IOException {
		super(args);
		this.chartId = Math.abs(new Random().nextInt());
	}
	
	public void addCanvasDiv(StringBuilder sb) {
		Chart.addCanvasDiv(chartId, sb);
	}
	
	public void addScriptContent(StringBuilder sb) {
		sb.append(String.format("var contextId%d = document.getElementById('canvasId%d').getContext('2d');\n", chartId, chartId));
		sb.append(String.format("var chartId%d = new Chart(contextId%d, {\n", chartId, chartId));

		// TODO: more clean up using J, but I'm tired of this.
		
		sb.append("type: 'scatter',\n");
		sb.append("data:\n");
		appendData(sb);
		
		//sb.append("options:\n");
		J options = J.curley("options")
				.add(J.field("pointDotRadius", "2"))
				.add(getElements())
				.add(Scales.get(ChartKind.POINT, 
			     		   new Axis(XorY.xAxes, xField.getName(), getXmax(), isXLog ? Scale.LOGARITHMIC : Scale.LINEAR),
			    		   new Axis(XorY.yAxes, yField.getName(), getYmax(), isYLog ? Scale.LOGARITHMIC : Scale.LINEAR)))						
				.add(getTitle())
				.add(getTooltips());
		sb.append(options);
		sb.append("});\n");

		/* 
		 * To make the point clickable to an ASIN, say
		 * https://codepen.io/jordanwillis/pen/bqvLNx
		 */
		StringBuilder func = new StringBuilder();
		func.append(String.format("document.getElementById('%s').onclick = function(evt){\n", Chart.getCanvasId(chartId)));
		func.append(String.format("var activePoint = chartId%d.getElementAtEvent(event);\n", chartId));

		// make sure click was on an actual point
		func.append("if (activePoint.length > 0) {\n");
		func.append("    var clickedDatasetIndex = activePoint[0]._datasetIndex;\n");
		func.append("    var clickedElementindex = activePoint[0]._index;\n");
		func.append(String.format("    var href = chartId%d.data.datasets[clickedDatasetIndex].data[clickedElementindex].href;\n", chartId));
		func.append("    if (href != null) {\n");
		func.append("       window.location.href = href;\n");
		func.append("     }\n");
		func.append("   }\n");
		func.append("};\n");
		
		sb.append(func);
		
	}

	/**
	 * 
	 * @return
	 */
	private J getElements() {
		J elems = J.curley("elements")
				.add(J.curley("line")
						.add(J.field("backgroundColor", "'rgba(0, 0, 0 ,0)'"))
						.add(J.field("borderColor", "'rgba(0, 0, 0 ,0)'"))
						.add(J.field("borderWidth", "0"))
						.add(J.field("fill", "false")));
		return elems;
	}
	
	private J getTooltips() {
		//  this function is the value of the field 'label'.
		StringBuilder func = new StringBuilder();
		func.append("function(tooltipItem, data) {\n");
        func.append("  var dataset = data.datasets[tooltipItem.datasetIndex];\n");
        func.append("  var label = dataset.data[tooltipItem.index].label;\n");
        func.append("  var display = '(' + tooltipItem.xLabel + ', ' + tooltipItem.yLabel + ')';\n");
        func.append("  if (label != null) { display += ' : ' + label;}\n");
        func.append("  return display;\n");        
        func.append("}");
		
		J tips = J.curley("tooltips")
				.add(J.curley("callbacks")
						.add(J.field("label", func.toString())));
		return tips;
	}
}
