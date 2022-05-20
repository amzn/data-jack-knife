package com.amazon.djk.report;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.amazon.djk.pipe.IfPipe;
import com.amazon.djk.processor.ExecutionContext;
import com.amazon.djk.reducer.Reducer;
import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.manual.Display;

/**
 * 
 * references:
 * https://en.wikipedia.org/wiki/Box-drawing_character
 * http://www.termsys.demon.co.uk/vtansi.htm
 */
public class GraphDisplay extends Display implements ReportDisplay {
	private enum NodeType {ROOT, INTERMEDIATE, TERMINAL};
	
	private boolean allowSuppression;
    private final StringBuilder displayBuilder = new StringBuilder();
    private int prevNumLines = 0;
    private int fudgeFactorLines = 1;
    private int numNodesDisplayed = 0; 
    
    public GraphDisplay() { 
    	allowSuppression = true;
    }
    
    public GraphDisplay(DisplayType type, boolean allowSuppression) {
    	super(type);
    	this.allowSuppression = allowSuppression;
    }
    
    @Override
    public void clear() {
        displayBuilder.setLength(0);
        numNodesDisplayed = 0;
    }
        
    private void addString(String string) {
        displayBuilder.append(string);
    }
        
    @Override    
    public void addLine(String line) {
        displayBuilder.append(line);
        displayBuilder.append('\n');
    }

	public String[] getAsLines() {
		String string = getAsString();
		return string.split("\n");
	}
	
    @Override
    public String getAsString() {
	    return actual2String();
	}

	private synchronized String actual2String() {
	    return displayBuilder.toString();
	}
	
    @Override
    public void render() {
        if (getDisplayType() == DisplayType.VT100) {
            // http://www.termsys.demon.co.uk/vtansi.htm
            int moveUp = prevNumLines + fudgeFactorLines;
            System.out.println ((char)27 + "[" + moveUp + "A"); // move up numLines from prev
            System.out.println ((char)27 + "[J"); // erase down
            if (fudgeFactorLines == 1) fudgeFactorLines++; // what can i say?
        }

        int numLines = 0;
        for (int i = 0; i < displayBuilder.length(); i++) {
            if (displayBuilder.charAt(i) == '\n') {
                numLines++;
            }
        }

        System.out.print(displayBuilder.toString());
        
        prevNumLines = numLines;
    }

    @Override
    public void render(File file) throws IOException {
        FileUtils.writeStringToFile(file, getAsString());
    }


    @Override
    public void appendFrom(ProgressReport report) {
    	numNodesDisplayed = 0;
        update(verticalConnector(), report, false);
    }
	
	/**
	 * 
	 * @param prefix
	 * @return
	 */
	private void update(String prefix, NodeReport report, boolean prevSuppressed) {
		// suppress this node ?
		boolean suppress = report.isSuppressed && allowSuppression;
		
		if (! suppress) {
			updateNode(prefix, report, prevSuppressed);
		} 
		
		if (!report.isLeftReportSuppressed()) {
			NodeReport sreport = report.getLeftReport();
			update(prefix, sreport, suppress);
		}
	}
	
	/**
	 * 
	 * @param prefix
	 * @return
	 */
	private void updateNode(String prefix, NodeReport report, boolean prevSuppressed) {
		numNodesDisplayed++;
		NodeType type = NodeType.ROOT;
		
		if (numNodesDisplayed > 1) {
			// spacing between us and the parent using the incoming prefix

			if (prevSuppressed) {
				String t2 = prefix.substring(0, prefix.length()-2) + dashedConnector();
				addLine(t2);
			} else {
				addLine(prefix);
			}
			
			type =	report.isLeftReportSuppressed() ? 
					NodeType.TERMINAL :
					NodeType.INTERMEDIATE;
			}

		ReportProvider rootProvider = report.rootProvider;
		String color = (rootProvider instanceof RecordSink) ? red() : 
					   (rootProvider instanceof Reducer) ? magenta() : 
					   (rootProvider instanceof Keyword) ? blue() : 
					   (rootProvider instanceof RecordPipe) ? yellow() : 
					   (rootProvider instanceof ExecutionContext) ? bold() :    
					   green();
		
		// print 1 line
		switch (type) {
		case ROOT: 
			// remove "| " from end, replace with "+-"
			String t1 = prefix.substring(0, prefix.length()-2) + LdownConnector();
			updateNodeName(t1, color, report);
			break;
			
		case INTERMEDIATE: 
			// remove "| " from end, replace with "+-"
			String t2 = prefix.substring(0, prefix.length()-2) + crossConnector();
			updateNodeName(t2, color, report);
			break;
			
		case TERMINAL:
			String t3 = prefix.substring(0, prefix.length()-2) + LupConnector(); // remove one set of "| "
			updateNodeName(t3, color, report);
			prefix = prefix.substring(0, prefix.length()-2) + "  ";
			break;
		}

		if (report.getProgressData() != null) {
		    updateNodeLines(prefix, report);
		}
		
		for (int i = 0; i < report.childReports.size(); i++) {
			NodeReport child = report.childReports.get(i);
			int numPrefixPipes = report.childReports.size() - i;
			String childPrefix = getPrefix(prefix, numPrefixPipes);
			update(childPrefix, child, false);
		}
	}
	
	/**
	 * 
	 * @param prefix
	 * @return number of lines displayed
	 */
	private int updateNodeLines(String prefix, NodeReport report) {
		int numLines = 0;
		List<String> lines = report.getProgressData().getLines();
		int numPrefixPipes = report.childReports.size();
		String linesPrefix = getPrefix(prefix, numPrefixPipes);
		
		for (String line : lines) { 
			if (line.length() == 0) continue;
			
			line = markNumbers(line);
				
			// print T F for first line of IfPipe
			if (report.rootProvider instanceof IfPipe && numLines == 0) {
				addString(getFirstIfPrefix(linesPrefix, report.childReports.size()));
			}
					
			else {
				addString(linesPrefix);
			}

			addLine(line);
			numLines++;
		}

		return numLines;
	}
	
	private String getPrefix(String prefix, int numPipes) {
		StringBuilder sb = new StringBuilder();
		sb.append(prefix);
		
		for (int i = 0; i < numPipes; i++) {
			sb.append(verticalConnector());
		}
		
		return sb.toString();
	}
	
	private void updateNodeName(String prefix, String color, NodeReport report) {
		if (prefix != null) addString(prefix);

		// thousands records/sec/thread
		String recsPerSec = markNumbers(String.format(" [%,2.1f]", report.getRecsPerSecond() / 1000.0F));
		
		if (getDisplayType() == DisplayType.DEFAULT) {
            addString(report.nodeLabel);
            
            ProgressData data = report.getProgressData();
            String header = data.getHeader();
            if (header != null && header.length() > 0) {
                 addString(" (");
                 addString(header);
                 addString(")");
             }

            if (recsPerSec != null) {
                addString(recsPerSec);
            }
            
            addLine("");
            return;
		}

		addString(color);
		addString(report.nodeLabel);
		addString(endColor());
		
        ProgressData data = report.getProgressData();
        String header = data.getHeader();
        if (header != null && header.length() > 0) {
		    addString(" (");
		    addString(markNumbers(header));
		    addString(")");
		}
			
		if (recsPerSec != null) {
		    addString(recsPerSec);
		}
		
		addLine("");
	}
	
	/**
	 * 
	 * @param linesPrefix
	 * @param numChildren
	 * @return
	 */
	private String getFirstIfPrefix(String linesPrefix, int numChildren) {
		// if 2 children --> T F else --> T
		int len = linesPrefix.length();
		return numChildren == 1 ? 
				linesPrefix.substring(0, len-2) + trueOnlyIf() :
				linesPrefix.substring(0, len-4) + trueFalseIf();
	}

	/**
	 * for %,d or %,f formats, convert them to bolding the 000s
	 * 
	 * @param inDisplay
	 * @return
	 */
	private String markNumbers(String inDisplay) {
		if (getDisplayType() != DisplayType.VT100) return inDisplay;
		
		String display = inDisplay;
		while (true) {
			int endMark = findEnd(display); // 0,000,000 -> 0,000E000
			if (endMark == -1) return display;
			display = display.substring(0, endMark) + 
					  endColor() + 
					  display.substring(endMark+1); // replace comma
			
			int beginMark = findBegin(endMark, display);
			if (beginMark == -1) return inDisplay; // error. return input
			if (display.charAt(beginMark) == ',') {
				display = display.substring(0, beginMark) + bold() +
						  display.substring(beginMark+1); // replace comma				
			} else {
				display = display.substring(0, beginMark+1) + bold() +
						  display.substring(beginMark+1); // insert
			}
		}
	}
	
	private int findEnd(String display) {
		int comma = display.lastIndexOf(',');
		if (comma == -1) return -1;
		
		if (comma < display.length()-1 && Character.isDigit(display.charAt(comma+1))) {
			return comma;
		}
		
		return -1;
	}
	
	private int findBegin(int endMark, String display) {
		for (int i = endMark-1; i >= 0; i--) {
			if (!Character.isDigit(display.charAt(i))) {
				return i;
			}
		}
		
		return -1;
	}
}
