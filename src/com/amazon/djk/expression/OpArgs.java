package com.amazon.djk.expression;

import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.SlotTokenizer.ParseSlot;
import com.amazon.djk.expression.SlotTokenizer.SlotEnd;
import com.amazon.djk.expression.SlotTokenizer.SlotType;
import com.amazon.djk.processor.KnifeProperties;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.Fields;
import com.amazon.djk.record.Pairs;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.Value;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * class the incorporates all the knowledge of parsing a predicate input token.
 * 
 * threadsafety required
 */
public class OpArgs {
    private static final Logger logger = LoggerFactory.getLogger(OpArgs.class);
    private final Map<String,Object> argMap = new HashMap<>();
    private final Map<String,Object> paramMap = new HashMap<>();
    private final Operator op;
    private final ParseToken token;
    private final ParserOperands operands;

    /**
     * Non params.source resolving version (for testing)
     *
     * @param op
     * @param opToken
     * @throws SyntaxError
     * @throws IOException
     */
    public OpArgs(Operator op, ParseToken opToken) throws SyntaxError, IOException {
        this(op, opToken, null);
    }

    /**
     * constructed by one thread, used across threads
     *
     * @param op
     * @param opToken
     * @param operands
     * @throws SyntaxError
     * @throws IOException
     */
    public OpArgs(Operator op, ParseToken opToken, ParserOperands operands) throws SyntaxError, IOException {
        OpUsage usage = op.getUsage();
        this.op = op;
        this.token = opToken;
        this.operands = operands;

        // params occur only in opToken slots, not in usage slots, process first and remove from list
        List<ParseSlot> tokenSlotsList = opToken.getSlots();

        //initialize the default values in map unless they are undefined
        HashMap<String, String> defaultValues = new HashMap<>();
        for ( String paramName: op.getParamNames()) {
            Param param = op.getParam(paramName);
            if (!param.defaultValue().equals(Param.UNDEFINED_DEFAULT)) {
                String value = KnifeProperties.resolveProperties(param.defaultValue());
                defaultValues.put(param.name(), value);
            }
        }

        processParams(tokenSlotsList, defaultValues);
        Iterator<ParseSlot> opSlots = tokenSlotsList.iterator();     
        Iterator<ParseSlot> usageSlots = usage.getSlots().iterator();     

        // use the usage slots to interpret the operator slots 
        while (usageSlots.hasNext()) {
            ParseSlot usageSlot = usageSlots.next();
            
            if (!opSlots.hasNext()) {
                throw new SyntaxError("'" + opToken.getString() + "' does not comply with '" + usage + "'");
            }
         
            switch (usageSlot.type) {
            case OPTIONAL_ARG:
            case ARG:
                putArgInMap(usageSlot.string, opSlots, !usageSlots.hasNext());
                break;
                
            case OPNAME_POSITION:
                // gobble up opSlot
                ParseSlot opSlot = opSlots.next();
                if (opSlot.type != SlotType.OPNAME_POSITION) {
                    throw new SyntaxError("'" + opToken.getString() + "' does not comply with '" + usage + "'");                    
                }
                break;

            case PARAM:
                // handled by processParams
            }
        }
    }

    /**
     * process the params from the params slot into the params map
     *  
     * @param tokenSlots
     * @throws SyntaxError
     * @throws IOException
     */
    private void processParams(List<ParseSlot> tokenSlots, HashMap<String, String> defaultValues) throws SyntaxError, IOException {
        // we should do more sophisticated parsing here using a library for encoding ...
        
        String additionalParamsSpec = null;

        // loop through slots processing params
        Iterator<ParseSlot> slots = tokenSlots.iterator();
        while (slots.hasNext()) {
            ParseSlot slot = slots.next();
            if (slot.type != SlotType.PARAM) continue;
            
            String[] nv = slot.string.split("=");
            if (nv.length != 2) throw new SyntaxError("error in params string");
                    
            if (nv[0].equals(Operator.ADDITIONAL_PARAMS)) {
                additionalParamsSpec = nv[1]; // save for last
            }

            else {
                putParamInMapIfAnnotated(nv[0], nv[1]);
                // if the user has set the param value then we will not be using related default values
                defaultValues.remove(nv[0]);
            }
        }
        
        if (additionalParamsSpec != null && !additionalParamsSpec.equals(Param.UNDEFINED_DEFAULT)) {
            if (additionalParamsSpec.equals(Operator.PARAMS_AS_INLINE_SOURCE)) {
                loadParamsFromSource(operands, defaultValues);
            } else {
                loadParamsFromFile(additionalParamsSpec, defaultValues);
            }
        }
        if (!defaultValues.isEmpty()) {
            putDefaultValuesInMap(defaultValues);
        }
    }

    private void putDefaultValuesInMap(HashMap<String, String> defaultValues) throws IOException {
        for (Map.Entry<String, String> entry : defaultValues.entrySet()) {
            if (entry.getValue().equals(Param.REQUIRED_FROM_COMMANDLINE)) {
                throw new IllegalArgumentException(String.format("Missing Required Parameter \"%s\" for operator \"%s\"",entry.getKey(), op.getName()));
            }
            putParamInMapIfAnnotated(entry.getKey(), entry.getValue());
        }
    }

    public boolean isInSubExpression() {
        return token.isInSubExpression();
    }
    
    public boolean isInMainExpression() {
        return !token.isInSubExpression();
    }

    public ParseToken getToken() {
        return token;
    }

    @Override
    // return just the args and params for easy report annotations
    public String toString() {
        return token.getArgsAndParams();
    }

    /**
     *
     * @param paramName
     * @param valueAsString
     * @throws SyntaxError
     * @throws IOException
     */
    private void putParamInMapIfAnnotated(String paramName, String valueAsString) throws SyntaxError, IOException {
        // everything is one-to-one except for PAIRS, FIELDS, STRINGS
        ArgType argType = op.getParamType(paramName);
        if (argType == null) { 
            logger.debug("attempt to add unannotated parameter '" + paramName + "'");
            return;  // not an annotated param
        }
        
        switch (argType) {
        case PAIRS:
            List<Field> names = new ArrayList<>();
            List<Value> values = new ArrayList<>();
            
            String[] pairs = valueAsString.split(",");
            for (String pair : pairs) {
                int colon = pair.indexOf(':');
                if (colon == -1) {
                    throw new SyntaxError("bad " + paramName + " param");                                        
                }
                names.add(new Field(pair.substring(0, colon)));
                values.add(new Value(pair.substring(colon+1)));
            }

            paramMap.put(paramName, new Pairs(names, values)); 
            break;
            
        case FIELDS:
            String[] fields = valueAsString.split(",");
            paramMap.put(paramName, new Fields(fields));
            break;
            
        case STRINGS:
            String[] strings = valueAsString.split(",");
            paramMap.put(paramName, strings);
            break;
            
        default: // one-to-one
            putInMap(paramMap, argType, paramName, valueAsString);
        }
    }

    private void loadParamsFromSource(ParserOperands operands, HashMap<String, String> defaultValues) throws SyntaxError, IOException {
        if (operands.size() == 0) {
            throw new SyntaxError("missing params source");
        }
        
        RecordSource paramsSource = operands.pop();
        Record paramsRecord = paramsSource.next();
        paramsSource.close();
        if (paramsRecord == null) return;  // no params to set
        
        FieldIterator fiter = new FieldIterator();
        fiter.init(paramsRecord);
        while (fiter.next()) {
            String name = fiter.getName();
            
            // skip those from operator which override
            Object existing = paramMap.get(name);
            if (existing != null) continue; 
            
            String value = fiter.getValueAsString();
            putParamInMapIfAnnotated(name, value);
            defaultValues.remove(name);
        }
    }
    
    /**                                                                                                                                    
     * load op properties from a file. any additional parameters given will OVERRIDE those 
     * in the file e.g: fooOp?op.params=myfile.properties&color=blue                                                                                
     * blue will override the value of the color property in myfile.properties.                                                            
     * @throws IOException                                                                                                                 
     * @throws SyntaxError 
     */
    private void loadParamsFromFile(String path, HashMap<String, String> defaultValues) throws IOException, SyntaxError {
        if (path == null) return;
        
        File file = new File(path);
        FileInputStream input = FileUtils.openInputStream(file);
        Properties props = new Properties();
        props.load(input);

        Set<Object> names = props.keySet();
        for (Object o : names) {
            String name = (String)o;
            
            // skip those from operator which override
            Object existing = paramMap.get(name);
            if (existing != null) continue; 
            
            String value = props.getProperty(name);
            putParamInMapIfAnnotated(name, value);
            defaultValues.remove(name);
        }

        input.close();
    }

    /**
     *
     * @param argName
     * @param opSlots
     * @throws SyntaxError
     * @throws IOException
     */
    private void putArgInMap(String argName, Iterator<ParseSlot> opSlots, boolean isLastUsageSlot) throws SyntaxError, IOException {
        // everything is one-to-one except for PAIRS, FIELDS, STRINGS
        ArgType argType = op.getArgType(argName);
        if (argType == null) {
            throw new SyntaxError("unknown argument: " + argName);
        }
        
        switch (argType) {
        case PAIRS:
            List<Field> names = new ArrayList<>();
            List<Value> values = new ArrayList<>();
            
            while (true) {
                ParseSlot n = opSlots.hasNext() ? opSlots.next() : null;
                ParseSlot v = opSlots.hasNext() ? opSlots.next() : null;
                
                if (n == null || v == null) {
                    throw new SyntaxError("bad " + argName + " arg");
                }
                    
                names.add(new Field(n.string));
                values.add(new Value(v.string));
                if (v.end != SlotEnd.COMMA) break;
            }
            
            argMap.put(argName, new Pairs(names, values));
            break;
            
        case FIELDS:
            // TODO: this is inconsistent.  The fields constructor should
            // take an array of Field.  FIXME:
            List<String> snames = new ArrayList<>();
            while (true) {
                ParseSlot f = opSlots.hasNext() ? opSlots.next() : null;
                
                if (f == null) {
                    throw new SyntaxError("bad " + argName + " arg");
                }
                
                snames.add(f.string);
                if (f.end != SlotEnd.COMMA) break;
            }

            argMap.put(argName, new Fields(snames.toArray(new String[0])));
            break;
            
        case STRINGS:
            List<String> strings = new ArrayList<>();
            while (true) {
                ParseSlot s = opSlots.hasNext() ? opSlots.next() : null;
                
                if (s == null) {
                    throw new SyntaxError("bad " + argName + " arg");
                }
                
                strings.add(s.string);
                if (s.end != SlotEnd.COMMA) break;
            }

            argMap.put(argName, strings.toArray(new String[0]));
            break;
            
            
        default: // one-to-one mappings
            StringBuilder sb = new StringBuilder();
            ParseSlot opSlot = opSlots.next();
            if (opSlot.type != SlotType.ARG) {
                throw new SyntaxError("'" + opSlot.string + "' does not comply with usage ");
            }

            sb.append(opSlot.string);
            
            // the case where there are more operator slots
            // than usage slots.  Here we concatenate the operator
            // slots back together and load into the last usage slot.
            ParseSlot last = opSlot;
            while (isLastUsageSlot && opSlots.hasNext()) {
                ParseSlot next = opSlots.next();
                if (next.type == SlotType.PARAM) break; // we're done with args

                sb.append(last.getEndAsChar());  // separator of last slot
                sb.append(next.string);
                last = next;
            }

            putInMap(argMap, argType, argName, sb.toString());
        }
    }

    /**
     *
     * @param name
     * @param valueAsString
     * @throws SyntaxError 
     * @throws IOException 
     */
    static void putInMap(Map<String,Object> map, ArgType type, String name, String valueAsString) throws SyntaxError, IOException {
        switch (type) {
        case STRING:
            map.put(name, valueAsString);
            break;

        case INTEGER:
            map.put(name, Integer.parseInt(valueAsString));
            break;

        case LONG:
            map.put(name, Long.parseLong(valueAsString));
            break;

        case DOUBLE:
            map.put(name, Double.parseDouble(valueAsString));
            break;
            
        case BOOLEAN:
            map.put(name, Boolean.parseBoolean(valueAsString));
            break;

        case FIELD:
            map.put(name, new Field(valueAsString));
            break;

        case FIELDS:
            map.put(name, new Fields(valueAsString));
            break;

        case VALUE:
            map.put(name, new Value(valueAsString));
            break;

        case PAIRS:
            throw new ProgrammingException("improper usage");

        default:
        }
    }

    /**
     *
     * @return
     * @throws IOException
     */
    public String getParamsAsString() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (String name : paramMap.keySet()) {
            Object value = paramMap.get(name);
            if (sb.length() != 0) {
                sb.append('&');
            }
            sb.append(name);
            sb.append('=');
            sb.append(value);
        }

        return sb.toString();
    }

    /**
     * need to return replicas of these objects when applicable since we are thread safe. 
     * 
     * @param name
     * @return
     */

    /**
     * need to return replicas of these objects when applicable since we are thread safe.
     *
     * @param name
     * @return
     */
    public Object getArg(String name) throws IOException {
        return get(argMap, name);
    }

    /**
     * need to return replicas of these objects when applicable since we are thread safe. 
     * 
     * @param name
     * @return
     */
    public Object getParam(String name) throws IOException {
        return get(paramMap, name);
    }
    
    /**
     * Even though defaultValue support is provided by OpArgs, this method provides
     * a way of specifying an additional default in the case where a default has not
     * been annotated for the parameter.
     * 
     * @param name
     * @param yetAnotherDefault
     * @return
     * @throws IOException
     */
    public Object getParam(String name, Object yetAnotherDefault) throws IOException {
        Object o = get(paramMap, name);
        return o != null ? o : yetAnotherDefault;
    }
    
    private Object get(Map<String,Object> map, String name) throws IOException {
        Object value = map.get(name);
        if (value == null) return null;
        
        // no replication needed
        if (value instanceof Boolean ||
            value instanceof String ||
            value instanceof Integer ||
            value instanceof Long ||
            value instanceof Double ||
            value instanceof Fields ||
            value instanceof String[]) {
            return value; 
        }
        
        else if (value instanceof Field) {
            return ((Field)value).replicate();
        } 
        
        else if (value instanceof Value) {
            return ((Value)value).replicate();
        }
        
        else if (value instanceof Pairs) {
            return ((Pairs)value).replicate();
        }

        else {
            return null;
        }
    }

    public Set<String> getParamNames() {
        return paramMap.keySet();
    }
    
    public Set<String> getArgNames() {
        return argMap.keySet();
    }
    
    /**
     * provides a way to add a parameter programmatically without Annotation on the Operator
     * 
     * @param name
     * @param value
     * @throws SyntaxError
     */
    public void addAnnotationLessParam(String name, Object value) throws SyntaxError {
        if (value instanceof Boolean ||
            value instanceof String ||
            value instanceof Integer ||
            value instanceof Long ||
            value instanceof Double ||
            value instanceof Field ||
            value instanceof Fields ||
            value instanceof Pairs ||
            value instanceof String[]) { 
            paramMap.put(name, value);
        }

        else throw new ProgrammingException("illegal Parameter value type");
    }
    
    /**
     * provides a way to add a arg programmatically without Annotation on the Operator
     * 
     * @param name
     * @param value
     */
    public void addAnnotationLessArg(String name, Object value) {
        if (value instanceof Boolean ||
            value instanceof String ||
            value instanceof Integer ||
            value instanceof Long ||
            value instanceof Double ||
            value instanceof Field ||
            value instanceof Fields ||
            value instanceof String[]) { 
            argMap.put(name, value);
        }
        
        else throw new ProgrammingException("illegal Argument value type");
    }
}
