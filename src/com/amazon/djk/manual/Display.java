package com.amazon.djk.manual;

import com.amazon.djk.processor.CoreDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Display {
	private static final Logger LOG = LoggerFactory.getLogger(Display.class);

	public enum DisplayType {VT100(0), HTML(1), DEFAULT(2);
		private final int id;
		DisplayType(int id) { this.id = id; }
		public int getId() { return id; }
		
		public static DisplayType getDisplayType(String typeAsString) {
			for (DisplayType type : DisplayType.values()) {
				if (type.toString().equals(typeAsString)) {
					return type;
				}
			}

			return DisplayType.DEFAULT;
		}
	}

	public DisplayType getDisplayType() {
		return type;
	}

	protected final DisplayType type;

	private static DisplayType getCurrentDisplayType() {
		DisplayType type = DisplayType.DEFAULT;
		try {
			type = CoreDefs.get().getDisplayType();
		} catch (IOException e) {
			LOG.warn("could not access CoreDefs, using default displayType", e);
		}

		return type;
	}
	
	private final static String[] DASHED_CONNECTOR = {"\u205e ", "<span style=\"color:red\">\u205e</span> ", "~ "};
	private final static String[] LUP_CONNECTOR = {"\u2514\u2500", "\u2514\u2500", "+-"};
	private final static String[] LDOWN_CONNECTOR = {"\u250c\u2500", "\u250c\u2500", "+-"};
	private final static String[] VERTICAL_CONNECTOR = {"\u2502 ", "\u2502 ", "| "};
	private final static String[] CROSS_CONNECTOR = {"\u251c\u2500", "\u251c\u2500", "|-"};
	private final static String[] TRUE_ONLY_IF = {"\u22a4 ", "\u22a4 ", "T "}; // logic true
	private final static String[] TRUE_FALSE_IF = {"\u22a4 \u22a5 ", "\u22a4 \u22a5 ", "T F "}; // logic true/false
	
	//public final static String HORZ_BAR = "\u2500";
	//public final static String RIGHT_ANGLE_LEFT_UP = "\u2518";

	private final static char e = (char)27;
	public final static String[] RED_COLOR = {e + "[1m" + e + "[31m", "<span style=\"color:red\">", ""};
	public final static String[] BLUE_COLOR = {e + "[1m" + e + "[34m", "<span style=\"color:blue\">", ""};
	public final static String[] LITE_BLUE_COLOR = {e + "[34m", "<span style=\"color:blue\">", ""};
	public final static String[] YELLOW_COLOR = {e +"[1m" + e + "[33m", "<span style=\"color:#ADAC37\">", ""};
	public final static String[] GREEN_COLOR = {e + "[1m" + e + "[32m", "<span style=\"color:green\">", ""};
	public final static String[] MAGENTA_COLOR = {e + "[1m" + e + "[35m", "<span style=\"color:magenta\">", ""};
	public final static String[] BOLD_COLOR = {e + "[1m", "<span style=\"font-weight:bold\">", ""};
	public final static String[] FAINT_COLOR = {e + "[2m", "<span style=\"font-weight:lighter\">", ""};
	public final static String[] END_COLOR = {e + "[0m", "</span>", ""};

	public Display(DisplayType type) {
		this.type = type;
	}

	public Display() {
		this(getCurrentDisplayType());
	}
	
	public String LupConnector() {
		return LUP_CONNECTOR[type.getId()];
	}
	
	public String LdownConnector() {
		return LDOWN_CONNECTOR[type.getId()];
	}
	
	public String verticalConnector() {
		return VERTICAL_CONNECTOR[type.getId()];
	}
	
	public String dashedConnector() {
		return DASHED_CONNECTOR[type.getId()];
	}
	
	public String crossConnector() {
		return CROSS_CONNECTOR[type.getId()];
	}
	
	public String trueOnlyIf() {
		return TRUE_ONLY_IF[type.getId()];
	}
	
	public String trueFalseIf() {
		return TRUE_FALSE_IF[type.getId()];
	}
	
	public String endColor() {
		return END_COLOR[type.getId()];
	}
	
	public String red() {
		return RED_COLOR[type.getId()];
	}
	
    public String blue() {
        return BLUE_COLOR[type.getId()];
    }
    
    public String lightblue() {
        return LITE_BLUE_COLOR[type.getId()];
    }

    public String yellow() {
        return YELLOW_COLOR[type.getId()];
    }
	
	public String green() {
		return GREEN_COLOR[type.getId()];
	}
	
	public String magenta() {
		return MAGENTA_COLOR[type.getId()];
	}
	
	public String bold() { return BOLD_COLOR[type.getId()]; }

	public String faint() { return FAINT_COLOR[type.getId()]; }
}
