package com.amazon.djk.chartjs;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.FieldType;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.NotIterator;
import com.amazon.djk.record.Record;

public abstract class ChartData {
	public final static String X_FIELD = "x";
	public final static String Y_FIELD = "y";
	public final static String HREF_FIELD = "href";
	public final static String DATASET_FIELD = "dataSet";
	public final static String TITLE_PARAM = "title";
	public static final String LOG_PARAM = "log";
	public static final String DEFAULT_TITLE = "No Title";
	protected final Field dataSetField;
	protected final Field xField;
	protected final Field yField;
	protected final Field href;
	protected final String title;
	protected final NotIterator notXYorSetName;
	protected final Color[] colors;

	protected final boolean isYLog;
	protected final boolean isXLog;
	
	private long xMax = Long.MIN_VALUE;
	private long yMax = Long.MIN_VALUE;
	
	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public ChartData(OpArgs args) throws IOException {
		 xField = (Field)args.getParam(X_FIELD);
		 yField = (Field)args.getParam(Y_FIELD);
		 href = (Field)args.getParam(HREF_FIELD);
		 dataSetField = (Field)args.getParam(DATASET_FIELD);
		 title = (String)args.getParam(TITLE_PARAM);
		 Fields others = new Fields(String.format("%s,%s,%s,%s", dataSetField, xField, yField, href));
		 notXYorSetName = others.getAsNotIterator();
		 
		 // keep this as STRINGS because its a better experience for the user
		 String logFields = StringUtils.join((String[])args.getParam(LOG_PARAM), ",");
		 isYLog = logFields.contains(Y_FIELD);
		 isXLog = logFields.contains(X_FIELD);

		 
		 // some hack to get started.
		 colors = new Color[] {
				 Color.rgba(10, 10, 255, 0.8F),	
				 Color.rgba(255, 10, 10, 0.8F),	
				 Color.rgba(10, 255, 10, 0.8F),	
				 Color.rgba(255, 125, 10, 0.8F),	
				 Color.rgba(10, 255, 125, 0.8F),	
				 Color.rgba(125, 10, 255, 0.8F),	
		 };
	}
	
	/**
	 * get a color from a built in pallet
	 * 
	 * @param dataSetNumber used modulo the number of colors
	 * @return
	 */
	protected Color getColor(int dataSetNo) {
		return colors[dataSetNo % colors.length];
	}
	
	private Object getValue(Field f, Record point) throws IOException {
		f.init(point);
		if (!f.next()) return null;
		
		FieldType type = f.getType();
		switch (type) {
		case STRING:
			return f.getValueAsString();

		case LONG:
			return f.getValueAsLong();

		case DOUBLE:
			return f.getValueAsDouble();

		default:
			return null;
		}
	}

	protected String getXStringValue(Record point) throws IOException {
		String value = point.getFirstAsString(xField);
		if (value == null) {
			throw new SyntaxError("data point has no '" + xField.getName() + "' field");			
		}
		
		return value;
	}

	private long getAsLong(Object o) {
		if (o instanceof Long) {
			return (long)o;			
		}

		double d = (Double)o;
		return (long)d;
	}

	protected Object getXvalue(Record point) throws IOException {
		Object o = getValue(xField, point);
		if (o == null) { 
			throw new SyntaxError("data point has no '" + xField.getName() + "' field");
		}
		xMax = Math.max(getAsLong(o), xMax);
		return o;
	}
	
	protected Object getYvalue(Record point) throws IOException {
		Object o = getValue(yField, point); 
		if (o == null) { 
			throw new SyntaxError("data point has no '" + yField.getName() + "' field");
		}
		yMax = Math.max(getAsLong(o), yMax);
		return o;
	}
	
	protected long getXmax() {
		return roundMax(xMax);
	}
	
	protected long getYmax() {
		return roundMax(yMax);
	}
	
	/**
	 * 
	 * @param max
	 * @return
	 */
	private long roundMax(long max) {
		double dmax = (double)max * 1.2; // 20% bigger
		return (long)((dmax/10.0) * 10.0); // rounded to the tens
	}
	
	public J getTitle() {
		return J.curley("title")
				.add(J.field("display", "true"))
				.add(J.field("fontSize", "20"))
				.add(J.field("text", String.format("'%s'", title)));
	}
	
	protected String getHref(Record point) throws IOException {
		return point.getFirstAsString(href);
	}
	
	protected String getOtherFieldsAsLabel(Record point) throws IOException {
		StringBuilder sb = new StringBuilder();
		notXYorSetName.init(point);
		while (notXYorSetName.next()) {
			if (sb.length() != 0) sb.append(",");
			sb.append(notXYorSetName.getName());
			sb.append(':');
			sb.append(notXYorSetName.getValueAsString());
		}

		return sb.length() != 0 ? sb.toString() : null;
	}
	
	public abstract void add(Record point) throws IOException;
	
	public abstract void appendData(StringBuilder sb);
}
