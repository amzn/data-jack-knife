package com.amazon.djk.report;

import java.io.File;
import java.io.IOException;

public interface ReportDisplay {
    
    /**
     * clears the display
     */
    public void clear();
    
    /**
     * appends the report to the display
     * @param report
     */
    public void appendFrom(ProgressReport report);
    
    /**
     * renders the display to stdout
     */
    public void render();
    
    /**
     * renders the display to a file
     * 
     * @param file
     */
    public void render(File file) throws IOException;

    /**
     * 
     * @param line
     */
    public void addLine(String line);
    
    /**
     * returns the display as a string
     * @return
     */
    public String getAsString();
}
