package com.amazon.djk.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.amazon.djk.expression.SyntaxError;

/**
 * This class is an adapter between the syntax for fields and the appropriate
 * Fields class.  It allows a predicate to to specify a FIELDS argument and 
 * instantiate either a Fields, NotFields,  It also implements wildcards for 
 * field names in terms of prefixes, e.g. price+
 * 
 *  threadsafe
 *
 */
public class Fields {
    private final String inputSpecification;
    private final boolean specifiedNegative;
    private final String[] fieldSpecs;
    private final List<String> fieldNames;
    private final List<Field> fieldsArray = new ArrayList<>();
    private final NamedFieldIterator allFields;
    public static final String ALL_FIELDS = "+";

    /**
     * 
     * @param inputSpecification comma separated list of field names. But there's more...
     * @throws IOException 
     */
    public Fields(String inputSpecification) throws IOException {
        this.inputSpecification = inputSpecification;
        this.specifiedNegative = inputSpecification.charAt(0) == '-';
        this.fieldSpecs = specifiedNegative ? 
                inputSpecification.substring(1).split(",") :
                inputSpecification.split(",");
                
         if (hasWildcards()) {
             fieldNames = new ArrayList<>();
             // these cause no new fields to be registered
             ThreadDefs.get().getFieldMatches(fieldSpecs, fieldNames);
         } 
         
         else {
             fieldNames = Arrays.asList(fieldSpecs);
         }
         
         if (isAllFields()) {
             fieldsArray.add(new Field());
         } else {
             for (String name : fieldNames) {
                 fieldsArray.add(new Field(name));
             }
         }
         
         allFields = new NamedFieldIterator(fieldNames);
    }

    /**
     * 
     * @param fields the field names
     * @throws IOException 
     */
    public Fields(String[] fields) throws IOException {
        this(StringUtils.join(fields, ","));
    }
    
    /**
     * constructor for ALL fields.
     * @throws IOException 
     */
    public Fields() throws IOException {
        inputSpecification = ALL_FIELDS;
        specifiedNegative = false;
        fieldSpecs = new String[]{ALL_FIELDS};
        fieldNames = Arrays.asList(fieldSpecs);
        fieldsArray.add(new Field());
        allFields = new NamedFieldIterator();
    }
    
    /**
     * 
     * @return true if any of the input fields specifies either a prefix or suffix wildcard. (e.g. foo+ or +foo)
     */
    public boolean hasWildcards() {
        return inputSpecification.indexOf('+') != -1;
    }
    
    public boolean isAllFields() {
        return inputSpecification.equals(ALL_FIELDS);
    }
    
    public List<Field> getAsFieldList() throws IOException {
        if (isAllFields()) return null;
        
        List<Field> list = new ArrayList<>();
        for (String name : fieldNames) {
            list.add(new Field(name));
        }
        
        return list;
    }
    
    /**
     * a FIELDS specification beginning with '-' specifies a NOT for the entire field list.
     * Allows an implementor to restrict or allow negative fields.
     * 
     * @return true if the field list was specified to be negative.
     */
    public boolean isSpecifiedAsNegative() {
        return specifiedNegative;
    }
    
    @Override
    public String toString() {
        return inputSpecification;
    }

    public FieldIterator getAsIterator() throws IOException {
        return specifiedNegative ?
                allFields.getAsNotIterator() : 
                (FieldIterator)allFields.replicate();
    }

    public NotIterator getAsNotIterator() throws IOException  {
        return allFields.getAsNotIterator();
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }
}
