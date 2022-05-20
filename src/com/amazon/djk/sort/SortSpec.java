package com.amazon.djk.sort;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.record.FieldType;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SortSpec {
    private final String spec;
    private final String fieldName;
    private final boolean ascending;
    private FieldType type = FieldType.STRING;
    private Pattern specPattern = Pattern.compile("(?<dir>[\\+\\-])(?<type>[lds])\\.(?<field>.*)");

    public SortSpec(String spec) throws SyntaxError {
        this.spec = spec;
        
        Matcher m = specPattern.matcher(spec);
        
        if (!m.find()) {
            throw new SyntaxError("expected {+-}{lsd}.FIELD, e.g. +l.count");
        }
        
        ascending = m.group("dir").charAt(0) == '+';
        String typeString = m.group("type");
        fieldName = m.group("field");
        
        switch (typeString) {
        case "l":
            type = FieldType.LONG;
            break;
            
        case "d":
            type = FieldType.DOUBLE;
            break;
            
        case "s":
            type = FieldType.STRING;
            break;
            
          default:
              throw new SyntaxError("expected [+-][lsd].FIELD, e.g. +l.count");
        }
    }
    
    @Override
    public String toString() {
        return spec;
    }
    
    /**
     * 
     * @return
     */
    public boolean isAscending() {
        return ascending;
    }
    
    /**
     * 
     * @return
     */
    public FieldType type() {
        return type;
    }
    
    /**
     * 
     * @return
     */
    public String fieldName() {
        return fieldName;
    }
}
