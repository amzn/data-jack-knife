package com.amazon.djk.expression;

import static com.amazon.djk.expression.Param.REQUIRED_FROM_COMMANDLINE;
import static com.amazon.djk.expression.Param.UNDEFINED_DEFAULT;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;

import com.amazon.djk.expression.SlotTokenizer.ParseSlot;
import com.amazon.djk.expression.SlotTokenizer.SlotEnd;
import com.amazon.djk.file.FileSystem;
import com.amazon.djk.file.SystemFileSystem;
import com.amazon.djk.format.FormatParser;
import com.amazon.djk.format.FormatWriter;
import com.amazon.djk.keyed.KeyedSink;
import com.amazon.djk.keyed.KeyedSource;
import com.amazon.djk.core.BaseRecordSource;
import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.MinimalRecordSource;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.processor.KnifeProperties;
import com.amazon.djk.processor.WithInnerSink;
import com.amazon.djk.reducer.Reducer;

/**
 * 
 * 
 */
@Param(name=Operator.ADDITIONAL_PARAMS, gloss = "{PATH, +} PATH = path to local properties file, + = first record of immediately preceding source. These can be overridden by explicit instances.", type=ArgType.STRING, eg="/tmp/params.properties")
public abstract class Operator {
    public final static String PARAMS_AS_INLINE_SOURCE = "+";
    public final static String ADDITIONAL_PARAMS = "op.params";
	private final String name;
	private final String usageString;
	private final OpUsage usage;
	
	private final List<String> paramNames = new ArrayList<>();
    private final Map<String,Param> paramMap = new HashMap<>();
    private final List<String> argNames = new ArrayList<>();
    private final Map<String,Arg> argMap = new HashMap<>();
	
	/**
	 * 
	 * @param usage specifies the syntax of the operator. e.g.
	 * 
	 * name
	 * name:ARG1
	 * ARG0=name:ARG1:ARG2?parm1=val1&parm2=val2
	 * ARG0,ARG1=name:ARG2:ARG3
	 * PATH
	 * 
	 * i.e. arguments must be specified all caps
	 * names must not be specified all caps
	 * 
	 * pattern in parser:
	 * 
	 * Operator op = getOp(input);
	 * OpArgs args = new OpArgs(op, input);
	 * Pipe pipe = op.getPipe(operands, opArgs);
	 * 
	 * or 
	 * 
	 * Source source = op.getSource(args);
	 * 
	 */
    public Operator(String usageString) {
        this.usageString = usageString;
        this.usage = new OpUsage(usageString);
        this.name = usage.getOpName();
    }
    
    public Operator(String name, String usageString) {
        this.usageString = usageString;
        this.usage = new OpUsage(usageString);
        this.name = name;
    }
	
	public String getName() {
		return name;
	}
	
	public String getUsageString() {
		return usageString;
	}
	
	public OpUsage getUsage() {
		return usage;
	}
	
	/**
	 * 
	 * @return the most 'specific' predicate type that this operator represents. 
	 */
	public Class<?> getMostSpecificPredicateType() {
        Class<?> predicateClass = getPredicateClass();
        if (predicateClass == null) return null;
        
        Class<?> superClass = predicateClass;
	    
        while (true) {
            // a hierarchy of clauses
            if (superClass == KeyedSink.class) return KeyedSink.class;
            if (superClass == RecordSink.class) return RecordSink.class;
            if (superClass == Reducer.class) return Reducer.class;
            if (superClass == RecordPipe.class) return RecordPipe.class;
            if (superClass == FormatParser.class) return FormatParser.class;
            if (superClass == FormatWriter.class) return FormatWriter.class;
            if (superClass == KeyedSource.class) return KeyedSource.class;
            if (superClass == MinimalRecordSource.class || 
            	superClass == BaseRecordSource.class) return RecordSource.class;
            if (superClass == FileSystem.class) return FileSystem.class;
            if (superClass == Object.class) return null;
            
            superClass = superClass.getSuperclass();
        }
	}
	
	public boolean isKeyword() {
	    Class<?> predicateClass = getPredicateClass();
	    if (predicateClass == null) return false;
	    
        Class<?> superClass = predicateClass;
        
        while (true) {
            for (Class<?> c : superClass.getInterfaces()) {
                if (c == Keyword.class) {
                    return true;
                }
            }
            
            superClass = superClass.getSuperclass();
            if (superClass == Object.class) return false;
        }
	}
	
	public boolean hasInnerSink() {
	    Class<?> predicateClass = getPredicateClass();
        if (predicateClass == null) return false;
        
        Class<?> superClass = predicateClass;
	    
        while (true) {
            for (Class<?> c : superClass.getInterfaces()) {
                if (c == WithInnerSink.class) {
                    return true;
                }
            }
            
            superClass = superClass.getSuperclass();
            if (superClass == Object.class) return false;
        }
	}
	
	public Class<?> getPredicateClass() {
	    Class<?> opClass = this.getClass();
	    Class<?> predClass = opClass.getDeclaringClass();
	    return predClass;
	}

	/**
	 * lazy to avoid this at constructor time
	 */
	private void lazySetParams() {
		if (paramNames.size() != 0) return;
		
        Class<?> clazz = this.getClass();
        while (true) {
            Annotation[] annos = clazz.getAnnotationsByType(Param.class);
            for (Annotation anno : annos) {
                Param param = (Param)anno;
                paramNames.add(param.name());
                paramMap.put(param.name(), param);
            }
              
            clazz = clazz.getSuperclass();
            if (clazz == Object.class) break;
        }
    }

	/**
	 * 
	 * @return the list of parameters for this operator
	 */
    public List<String> getParamNames() {
    	lazySetParams();
        return paramNames;
    }

    /**
     * 
     * @param paramName
     * @return the ArgType of this param
     */
	public ArgType getParamType(String paramName) {
		lazySetParams();
        Param param = paramMap.get(paramName);
        if (param == null) return null;
        return param.type();
	}

	/**
	 * 
	 * @param paramName
	 * @return the param
	 */
	public Param getParam(String paramName) {
		lazySetParams();
		return paramMap.get(paramName);
    }

	/**
	 * lazy to avoid this at constructor time
	 */
	private void lazySetArgs() {
		if (argNames.size() != 0) return;
		
        Class<?> clazz = this.getClass();
        while (true) {
            Annotation[] annos = clazz.getAnnotationsByType(Arg.class);
            for (Annotation anno : annos) {
                Arg arg = (Arg)anno;
                argNames.add(arg.name());
                argMap.put(arg.name(), arg);
            }
              
            clazz = clazz.getSuperclass();
            if (clazz == Object.class) break;
        }
    }

	/**
	 * 
	 * @return list of arg names for this operator
	 */
    public List<String> getArgNames() {
    	lazySetArgs();
        return argNames;
    }
    
    /**
     * 
     * @param argName
     * @return
     */
	public ArgType getArgType(String argName) {
    	lazySetArgs();
        Arg arg = argMap.get(argName);
        if (arg == null) return null;
        return arg.type();
    }

	/**
	 * 
	 * @param argName
	 * @return
	 */
	public Arg getArg(String argName) {
    	lazySetArgs();
    	return argMap.get(argName);
    }
	
	/**
	 * 
	 * @return
	 */
    public List<Gloss> getGlosses() {
    	List<Gloss> glosses = new ArrayList<>();
    	
        Class<?> clazz = this.getClass();
        while (true) {
            Annotation[] annos = clazz.getAnnotationsByType(Gloss.class);
            for (Annotation anno : annos) {
                Gloss gloss = (Gloss)anno;
                glosses.add(gloss);
            }
              
            clazz = clazz.getSuperclass();
            if (clazz == Object.class) break;
        }
        
        return glosses;
    }
    
    /**
     * 
     * @return
     */
    public String getUsageExample() {
	    StringBuilder sb = new StringBuilder();
	    
	    Class<?> predicateType = getMostSpecificPredicateType();
	    
	    OpUsage usage = getUsage();
	    List<ParseSlot> slots = usage.getSlots();
	    
	    for (ParseSlot slot : slots) {
	    	switch (slot.type) {
	    	
	    	case ARG:
	    		String argName = slot.string;
                Arg arg = getArg(argName);
                appendValue(sb, arg);	    		
	    		
                // this is the form OUTPUT=op:A:B
                if (slot.end == SlotEnd.EQUALS) {
                	sb.append('=');
                }
                
                else if (slot.end == SlotEnd.COLON) {
                	sb.append(':');
                }
                
                else if (slot.end == SlotEnd.NONE) {
                	break;
                }
                
                else {
                	throw new RuntimeException("Unhandled usage example.  Fix the code");
                }
	    		break;
	    		
	    	case OPNAME_POSITION:
	    		// kinda broken
	    		boolean isFileSystem = (predicateType != null && predicateType.equals(FileSystem.class));
	    		if (isFileSystem) {
	    			sb.append(getName());
    				sb.append("://");
    				if (!getName().equals(SystemFileSystem.SCHEME)) { // no path for stdin
        				sb.append("PATH");
    				}
    				
	    		} else {
	    			sb.append(slot.string);
	    			if (slot.end == SlotEnd.COLON) {
	    				sb.append(':');
	    			}
	    		}
	    		break;
	    		
	    	case PARAM:
	    	case OPTIONAL_ARG:
	    		default:
	    	}
	    }

	    List<String> paramNames = getParamNames();
	    if (paramNames.size() > 1) { // need more than 'op.params'
	        char delim = '?';
	        for (String name : paramNames) {
	            sb.append(delim);
	            Param param = getParam(name);
	            sb.append(name);
	            sb.append('=');
                appendValue(sb, param);
	            delim = '&';
	        }
	    }
	    
	    return sb.toString();
	}
    
    /**
	 * for building up an example to display 
	 * 
	 * @param sb
	 * @param o
	 */
	private void appendValue(StringBuilder sb, Object o) {
	    ArgType type = null;
	    if (o instanceof Arg) {
            Arg a = (Arg)o;
            if (!a.eg().equals("")) {
                sb.append(a.eg());
                return;
            }
	        
            type = a.type();
	    }
	    
	    else if (o instanceof Param) {
	        Param p = (Param)o;

	        if (!p.defaultValue().equals(UNDEFINED_DEFAULT) && !p.defaultValue().equals(REQUIRED_FROM_COMMANDLINE) && parseParamDefaultValues(p.defaultValue()) != null) {
	        	sb.append(parseParamDefaultValues(StringEscapeUtils.escapeJava(p.defaultValue())));
	        	return;
	        }
	        
	        if (!p.eg().equals("")) {
	        	sb.append(p.eg());
	            return;
	        }

	        type = p.type();
	    }
	    
	    switch (type) {
	    case BOOLEAN:
            sb.append("true");
	        break;
	        
        case DOUBLE:
            sb.append("1.0");
            break;
            
        case LONG:
            sb.append("2");
            break;
            
        case FIELD:
            sb.append("id");
            break;
            
        case FIELDS:
            sb.append("id,color");
            break;
            
        case PAIRS:
            sb.append("size:big,color:blue");
            break;
        
        case STRING:
            sb.append("tree");
            break;

        case STRINGS:
            sb.append("foo,bar,baz");
            break;
            
        case VALUE:
            sb.append("'{b.isBig;}'");
            break;

        default:
            sb.append("foo");
            break;
	    }
	}

	/**
	 * 
	 * @param defaultValue
	 * @return
	 */
	private String parseParamDefaultValues(String defaultValue) {
		if (KnifeProperties.isSystemProperty(defaultValue)) {
			return KnifeProperties.getDefaultValue(defaultValue);
		}
		return defaultValue;
	}
}
