package com.amazon.djk.processor;

import com.amazon.djk.expression.ChunkTokenizer;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.ManPage;
import com.amazon.djk.record.ThreadDefs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnifeProperties {
	private static final Logger logger = LoggerFactory.getLogger(KnifeProperties.class);
	
	public static final char NAME_SPACE_PROPERTY_DELIM = ':';
	public static final String SWITCH_DEFAULT = "DEFAULT";
	public static final String PROPERTY_NAME = "name";
	
	public static class Namespace {
		private final String namespace;

		public Namespace() {
			namespace = String.format("namespace%d", Math.abs(new Random().nextInt()));
	    }

		public Namespace(String namespace) {
			this.namespace = namespace;
		}
		
		public Namespace(Namespace superspace, String subspace) {
			namespace = String.format("%s%c%s", superspace.namespace, NAME_SPACE_PROPERTY_DELIM, subspace);
		}
		
		/**
		 * 
		 * @param namespace
		 * @param takeSubspace if true, create a namespace out of the subspace of this namespace
		 */
		
		/**
		 * 
		 * @param namespace
		 * @param takeSuperspace if true, creates a new namespace out of the input namespace
		 * with the right-most subspace removed 
		 */
		public Namespace(Namespace namespace, boolean takeSuperspace)  {
			if (!takeSuperspace) {
				this.namespace = namespace.namespace;
			} else {
				int colon = namespace.namespace.lastIndexOf(NAME_SPACE_PROPERTY_DELIM);
				if (colon == -1) {
					this.namespace = ""; // the empty namespace
				} else {
					this.namespace = namespace.namespace.substring(0, colon);
				}
			}
		}
		
	    /**
	     * Combines the namespace with the property name to create 'namedspaced' name.
	     * 
	     * @param name
	     * @param propertiesNamespace
	     * @return
	     */
	    private String getNamespacedName(String name) {
	    	return String.format("%s%c%s", namespace, NAME_SPACE_PROPERTY_DELIM, name);
	    }
	    
	    /**
	     * The superspace of a namespace is that namespace with its right-most
	     * subspace removed
	     * 
	     * @return the superspace of this namespace or null if none exists.
	     * @throws SyntaxError 
	     */
	    public Namespace getSuperspace() throws SyntaxError {
	    	int colon = namespace.lastIndexOf(NAME_SPACE_PROPERTY_DELIM);
	    	if (colon == -1) return null;
	    	return new Namespace(this, true);
	    }
	    
	    @Override
	    public String toString() {
	    	return namespace;
	    }
	}

	/* DJK uses a hierarchical namespace where the path separator is :
	 * If DJK fails to find a property under the entire namespace:name it will
	 *  
	 * remove a subspace from the current namespace and try recursively.
	 */
	public static String getProperty(Namespace namespace, String name) throws SyntaxError {
		String namespacedName = namespace.getNamespacedName(name);
		
		String value = System.getProperty(namespacedName);
		if (value != null) return value;

		Namespace superspace = namespace.getSuperspace();
		if (superspace == null) { // no hierarchy within the namespace so try name alone.
			return System.getProperty(name);			
		}
		
		// there is a superspace, try it recursively
		return getProperty(superspace, name); // recurse
	}
	
	/**
	 * 
	 * @param namespace the namespace within which to set the property value
	 * @param name of the namespace
	 * @param unresolvedValue the unresolved value
	 * @return
	 * @throws SyntaxError
	 */
	public static String setProperty(Namespace namespace, String name, String unresolvedValue) throws SyntaxError {
		if (name.contains(":")) {
			throw new SyntaxError("property name must not contain a colon.");
		}
		
		String resolvedValue = resolveProperties(namespace, unresolvedValue);
		if (resolvedValue.contains(" ")) {
			throw new SyntaxError("properties are not allowed to contain spaces.");
		}
		
		String namespacedName = namespace.getNamespacedName(name);;
		System.setProperty(namespacedName, resolvedValue);
		return resolvedValue;
	}

	/**
	 *
	 * @param namespace the namespace within which to remove the property
	 * @param name of the namespace
	 * @return
	 * @throws SyntaxError
	 */
	public static void clearProperty(Namespace namespace, String name) {
		String namespacedName = namespace.getNamespacedName(name);
		System.clearProperty(namespacedName);
	}

	
    private static final Pattern sysPropPat = Pattern.compile("\\$\\{(?<content>[^{}\\$]+)\\}");
    private static final Pattern namePat = Pattern.compile("\\s*(?<name>[A-Za-z0-9_\\.-]+)\\s*");
    private static final Pattern nameAndDefaultPat = Pattern.compile("\\s*(?<name>[A-Za-z0-9_\\.-]+)\\s*:\\s*(?<default>[^\\s]+)\\s*");
    private static final Pattern switchPat = Pattern.compile("\\s*(?<name>[A-Za-z0-9_\\.-]+)\\s*\\?\\s*(?<rest>.*)");

    public static String resolveProperties(String chunk) throws IOException {
    	return resolveProperties(ThreadDefs.get().getPropertiesNamespace(), chunk);
	}
    
    /**
     * resolves property references within a string, of the form:
     * 
     * name : ${NAME}
     * name with default : ${NAME:defaultifnonexistant}
     * switch: ${NAME ? ford:green honda:blue toyota:yellow DEFAULT:pink} 
     * 
     * @param chunk
     * @param propertiesNamespace
     * @return
     * @throws SyntaxError
     */
    public static String resolveProperties(Namespace namespace, String chunk) throws SyntaxError {
        StringBuffer sb = new StringBuffer();
        Matcher m = sysPropPat.matcher(chunk);
        while (m.find()) {
            String curleyContent = m.group("content");

            // ${FOO} 
            Matcher next = namePat.matcher(curleyContent);
            if (next.matches()) {
            	String name = next.group(PROPERTY_NAME);
            	String value = getProperty(namespace, name);
                if (value == null) {
                    throw new SyntaxError(String.format("'%s' property is unresolved in the '%s' namespace", name, namespace));
                }
                
                m.appendReplacement(sb, value);

                logger.info(String.format("resolving ${%s} to %s (because property %s=%s in namespace=%s)", curleyContent, value, name, value, namespace));                
                continue;
            }

            // ${FOO : mydefaultvalue}
            next = nameAndDefaultPat.matcher(curleyContent);
            if (next.matches()) {
            	String name = next.group(PROPERTY_NAME);
            	String def = next.group("default");

            	String value = getProperty(namespace, name);
            	if (value == null) {
            		// values beginning with @ mean use the following property
            		value = def.charAt(0) == '@' ?
            				getProperty(namespace, def.substring(1)) : def;
            	}
            	
            	m.appendReplacement(sb, value);
            	
                logger.info(String.format("resolving ${%s} to %s (because property %s=%s in namespace=%s)", curleyContent, value, name, value, namespace));
                continue;
            }

            // ${NAME ? ford:green honda:blue toyota:yellow DEFAULT:pink} 
            next = switchPat.matcher(curleyContent);
            if (next.matches()) {
            	String name = next.group(PROPERTY_NAME);
            	String rest = next.group("rest");

            	String nameValue = getProperty(namespace, name);
            	String switchValue = getSwitchValue(nameValue, rest, namespace);
            	if (switchValue == null) {
            		throw new SyntaxError(String.format("bad property switch reference: ${%s}", curleyContent));
            	}
            	
            	m.appendReplacement(sb, switchValue);
                logger.info(String.format("resolving ${%s} to %s (because property %s=%s in namespace=%s)", curleyContent, switchValue, name, nameValue, namespace));            	
            	continue;            		
            }
            
            throw new SyntaxError(String.format("bad property reference: ${%s}", curleyContent));
        }
        m.appendTail(sb);
        
        return sb.toString();
    }
    
    
    /**
     * resolve the properties switch reference.  Values that begin with '@' mean use the property
     * that follows e.g. '@foo' the value is the value of property 'foo' 
     * 
     * @param caseSwitch the parameter to switch on.
     * @param caseValues the case values, e.g. 'ford:green honda:red DEFAULT:purple' 
     * @return the value of the case statement
     * @throws SyntaxError
     */
    private static String getSwitchValue(String caseSwitch, String caseValues, Namespace namespace) throws SyntaxError {
    	String[] pairs = ChunkTokenizer.split(caseValues); // splits on space
    	Map<String,String> map = new HashMap<>();
    	
    	// could use the SlotTokenizer here if this syntax gets complicated
    	for (String pair : pairs) {
    		int colon = pair.indexOf(':');
    		if (colon == -1) {
    			return null;
    		}

    		String name = pair.substring(0, colon);
    		
    		// values beginning with @ mean use the following property
    		String value = pair.charAt(colon+1) == '@' ?
    				getProperty(namespace, pair.substring(colon+2)) :
    				pair.substring(colon+1);
    		
    		map.put(name, value); 
    	}
    	
    	if (map.get(SWITCH_DEFAULT) == null) {
    		return null;
    	}
    	
    	String switchValue = map.get(caseSwitch);
    	return switchValue != null ? switchValue : map.get(SWITCH_DEFAULT);
    }
    
    public static boolean isSystemProperty(String name) {
    	Matcher m = sysPropPat.matcher(name);
    	return m.find();
	}

	public static String getPropertyName(String chunk) {
		Matcher m = sysPropPat.matcher(chunk);
		while (m.find()) {
			String curleyContent = m.group("content");

			// ${FOO}
			Matcher next = namePat.matcher(curleyContent);
			if (next.matches()) {
				return next.group(PROPERTY_NAME);
			}

			// ${FOO : mydefaultvalue}
			next = nameAndDefaultPat.matcher(curleyContent);
			if (next.matches()) {
				return next.group(PROPERTY_NAME);
			}

			// ${NAME ? ford:green honda:blue toyota:yellow DEFAULT:pink}
			next = switchPat.matcher(curleyContent);
			if (next.matches()) {
				return next.group(PROPERTY_NAME);
			}
		}
		return null;
	}

	public static String getDefaultValue(String chunk) {
    	Matcher m = sysPropPat.matcher(chunk);
    	while (m.find()) {
    		String curleyContent = m.group("content");
    		Matcher next = nameAndDefaultPat.matcher(curleyContent);
    		if (next.matches()) {
    			return next.group("default");
			}
		}
		return null;
	}
	
	/**
	 * 
	 *
	 */
	@Description(topic="Properties", text = { "Properties allow you to define STRINGS for replacement within expressions.",
	"They are evaluated before the expression runs unless they are accessed via the VALUE type.  ",
	"Properties don't allow spaces and therefore don't require quotes. They are defined preceding ",
	"the expression with a terminating semi-colon or can be passed to the JVM (-Dhost=bigbox without semi-colon). ",
	"Properties can reference other properties."},
	preLines={
	        "Properties can be referenced in the following ways:",
	        "* like bash variable: ${myProperty}",
	        "* with-default if non-existent: ${myProperty : defaultValue}",
	        "* switch-like syntax: ${myProperty ? case1:value1 case2:value2 DEFAULT:defaultValue}",
	        "",
	        "Use the @ symbol to reference property values indirectly within the with-default and switch-like references.",
	        "",
	        "NOTE: In order to promote expression readability, properties are not allowed to contain spaces",
	        "Doing so prevents using properties to inject large snippets of djk expression into a djk macro.",
	    }
	)
	@Example(expr = "color=blue; [ id:1 ] add:color:${color}", type=ExampleType.EXECUTABLE)
	@Example(expr = "color=blue; car=${color}-toyota; [ id:1 ] add:car:${car}", type=ExampleType.EXECUTABLE)
	@Example(expr = "[ id:1 ] add:size:${size : small}", type=ExampleType.EXECUTABLE)
	@Example(expr = "OTHERWISE=big; [ id:1 ] add:size:${size : @OTHERWISE}", type=ExampleType.EXECUTABLE)
	@Example(expr = "day=tuesday; [ id:1 ] add:flavor:${day ? monday:chocolate tuesday:cherry DEFAULT:vanilla}", type=ExampleType.EXECUTABLE)
	@Example(expr = "day=wednesday; OTHERWISE=peach; [ id:1 ] add:flavor:${day ? monday:chocolate tuesday:cherry DEFAULT:@OTHERWISE}", type=ExampleType.EXECUTABLE)
	public static class Entry extends ManPage {
		public Entry() {
			super("Properties");
		}
	}
}
