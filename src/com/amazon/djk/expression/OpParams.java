package com.amazon.djk.expression;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;

/**
 * 
 * FIXME: Currently unused static methods??
 */
public class OpParams {
    public static final String FIELDS_PARAM = "fields";
    
    private static class ParamPair {
        public final String name;
        public final ArgType type;
        
        public ParamPair(String name, ArgType type) {
            this.name = name;
            this.type = type;
        }
    }

    // params that don't need to be defined on a particular operator
    private static List<ParamPair> globalParams = new ArrayList<>();
    static {
        globalParams.add(new ParamPair(Operator.ADDITIONAL_PARAMS, ArgType.STRING));
    }
    
    /**
     * 
     * @return
     * @throws SyntaxError 
     * @throws IOException 
     */
    public static Map<String,Object> getParamMap(Operator op, String params) throws SyntaxError, IOException {
        Map<String,Object> paramMap = new HashMap<>();
        if (params == null) return paramMap;
        
        // allow ? for split too for easy string based concatenation
        String[] pairs = params.split("[&\\?]");
        for (String pair : pairs) {
            String[] nv = pair.split("=");
            if (nv.length == 2) {
            	addToParamMap(op, paramMap, nv[0], nv[1]);            	
            }
            
            // allow foo?isFunny to mean foo?isFunny=true
            // with throw SyntaxError if isFunny is not Boolean
            // NOTE: this is not enough to make this work
            else if (nv.length == 1){
            	addToParamMap(op, paramMap, nv[0], "true");            	
            }
        }
        
        loadFileParams(op, paramMap);
        
        return paramMap;
    }
    
    /**
     * 
     * @param name
     * @param valueAsString
     * @throws SyntaxError
     * @throws IOException 
     */
    private static void addToParamMap(Operator op, Map<String,Object> paramMap, String name, String valueAsString) throws SyntaxError, IOException {
        ArgType type = op.getParamType(name);
            
        if (type == null) {
            // list of exception params that dont require being defined on an op
            for (ParamPair entry : globalParams) {
                if (name.equals(entry.name)) {
                    OpArgs.putInMap(paramMap, entry.type, name, valueAsString);
                    return;
                }
            }
            
            // else don't put this unknown param.
            return;
            //throw new ProgrammingException("@PARAM.name()=" + name + " not defined for operator:" + op.getName());          
        }
            
        OpArgs.putInMap(paramMap, type, name, valueAsString);
    }
    
    /**
     * load op properties from a file. any additional parameters given in the parse arguments
     * will OVERRIDE those in the file e.g: 
     * foo?op.params=my.params.file&color=blue
     * blue will override the value of the color property in my.params.file
     * @throws IOException
     * @throws SyntaxError 
     */
    private static void loadFileParams(Operator op, Map<String,Object> paramMap) throws IOException, SyntaxError {
        Object path = paramMap.remove(Operator.ADDITIONAL_PARAMS);
        if (path == null) return;

        File file = new File((String)path);
        FileInputStream input = FileUtils.openInputStream(file);
        Properties props = new Properties();
        props.load(input);
        
        Set<Object> names = props.keySet();
        for (Object o : names) {
            String name = (String)o;
            
            Object existing = paramMap.get(name);
            if (existing != null) continue; 

            ArgType type = op.getParamType(name);
            String valueAsString = props.getProperty(name);
            OpArgs.putInMap(paramMap, type, name, valueAsString);
        }
        
        input.close();
    }
}
