package com.amazon.djk.chartjs;

public class Scales extends JCurley {
	public enum ChartKind {BAR, STACKED, POINT};
	public enum Scale {BAR, LINEAR, LOGARITHMIC};
	private final ChartKind kind;
	private final Axis xAxis;
	private final Axis yAxis;	
	
	public static class Axis {
		public enum XorY {xAxes, yAxes};
		public final XorY xOrY;
		public final String label;
		public final long max;
		public final Scale scale;
		
		public Axis(XorY xOrY, String label, long max, Scale scale) {
			this.xOrY = xOrY;
			this.label = label;
			this.max = max;
			this.scale = scale;
		}
	}
	
	public static Scales get(ChartKind kind, Axis x, Axis y) {
		return new Scales(kind, x, y);
	}
	
	private Scales(ChartKind kind, Axis x, Axis y) {
		super("scales");
		this.kind = kind;
		this.xAxis = x;
		this.yAxis = y;
	}
	
	@Override
	public String toString() {
		switch (xAxis.scale) {
		case BAR:
			add(getVbarAxis(xAxis));
			break;
		case LINEAR:
			add(getLinearAxis(xAxis));
			break;
		case LOGARITHMIC:
			add(getLogarithmicAxis(xAxis));
		}
		
		add(yAxis.scale == Scale.LINEAR ? getLinearAxis(yAxis) : getLogarithmicAxis(yAxis));		
		return super.toString();
	}

	private J getLinearAxis(Axis axis) {
		return J.square(axis.xOrY.toString())
				.add(J.curley()
					.add(J.field("display", "true"))
					.add(J.field("type", "'linear'"))
					.add(J.field("ticks", String.format("{suggestedMin: %d, max:%d}", 0, axis.max)))
					.add(getScaleLabel(axis.label))
				);
	}
	
	private J getVbarAxis(Axis axis) {
		return J.square(axis.xOrY.toString())
				.add(J.curley()
					.add(J.field("display", "true"))
					.add(getScaleLabel(axis.label))
				);
	}
	
	private J getLogarithmicAxis(Axis axis) {
		StringBuilder func = new StringBuilder();
		func.append("function(tick) {\n");
		func.append("  var remain = tick / (Math.pow(10, Math.floor(Chart.helpers.log10(tick))));\n");
		func.append("  if (remain === 1 || remain === 2 || remain === 5) {\n");
		func.append("    return tick.toString();\n");
		func.append("  }\n");
		func.append("  return ''\n");
		func.append("}\n");

		return J.square(axis.xOrY.toString())
				.add(J.curley()
					.add(J.field("display", "true"))
					.add(J.field("type", "'logarithmic'"))
					.add(J.curley("ticks")
						.add(J.field("max", axis.max))
						.add(J.field("userCallback", func.toString()))
					)
					.add(getScaleLabel(axis.label))
				);
	}	
	
	private J getScaleLabel(String label) {
		return J.curley("scaleLabel")
					.add(J.field("display", "true"))
					.add(J.field("fontSize", 20))
					.add(J.field("labelString", String.format("'%s'", label))
					);
	}
}
