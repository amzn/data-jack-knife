package com.amazon.djk.expression;

import java.util.Iterator;
import java.util.List;

/**
 * An 'internal' macro is one defined using a text file, included in the com.amazon.djk.processor 
 * directory.  To replace the description available with Operator classes, this class parses it
 * out of the text file (somewhat simplified), e.g.
 * 
 * @Description this is the description
 * @Usage this is the usage
 * 
 * This class is begging to be unified with Operator annotations but its a lot of work.
 * 
 */
public class InternalMacroOperator extends MacroOperator {
    private final static String DESCRIPTION_ANNO = "@Description";
    private final static String USAGE_ANNO = "@Usage";
    private final List<String> rawMacroLines;
    private final String description;
    private final String usage;
    /**
     * @param name
     * @param name the name of the macro, for which there is a corresponding file: name.djk within
     * the com.amazon.djk.processor directory.
     */
    public InternalMacroOperator(String name, List<String> rawMacroLines) {
        super(name);
        this.rawMacroLines = rawMacroLines;
        
        String parsedDescription = null;        
        String parsedUsage = null;        
        Iterator<String> it = rawMacroLines.iterator();
        while (it.hasNext()) {
            String line = it.next();
            if (line.startsWith(DESCRIPTION_ANNO)) {
                parsedDescription = line.substring(DESCRIPTION_ANNO.length()).trim();
                it.remove();
            }
            
            else if (line.startsWith(USAGE_ANNO)) {
                parsedUsage = line.substring(USAGE_ANNO.length()).trim();
                it.remove();
            }
        }
     
        description = (parsedDescription != null) ? parsedDescription : 
            String.format("Missing description! use %s within %s.djk", DESCRIPTION_ANNO, name); 
        
        usage = (parsedUsage != null) ? parsedUsage : 
            String.format("Missing usage! use %s with %s.djk", USAGE_ANNO, name);         
    }
    
    public String getDescription() {
        return description;
    }
    
    /**
     * @return
     */
    public String getMacroUsage() {
        return usage;
    }
    
    @Override
    public List<String> getRawMacroLines(OpArgs args) {
        return rawMacroLines;
    };
}
