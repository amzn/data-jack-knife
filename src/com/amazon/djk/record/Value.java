package com.amazon.djk.record;

import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.format.ReaderFormatParser;
import com.amazon.djk.java.ValueFunction;
import com.amazon.djk.java.ValueFunctionClassFactory;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.ManPage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Value {
    private static final Logger logger = LoggerFactory.getLogger(Value.class);
    public enum ValueType {PRIMITIVE, DJK_JAVA, INDIRECT_FIELD};
	private final String inputString;
	private final Field indirectField;
	private final ValueType type;
    private final Class<?> valueFunctionClass;
    private final String valueFunctionCode;
    private final ValueFunction valueFunction;
    
	public Value(String inputValueAsString) throws SyntaxError, IOException {
		this.inputString = inputValueAsString;
		
		if (inputString.length() > 2 && inputString.charAt(0) == '{' && 
		    inputString.charAt(inputString.length()-1) == '}') {
            type = ValueType.DJK_JAVA;
            indirectField = null;
		    String djkJava = inputString.substring(1, inputString.length()-1);

		    try {
		        valueFunctionCode = ValueFunctionClassFactory.getCode(djkJava);
                valueFunctionClass = ValueFunctionClassFactory.getClass(valueFunctionCode);
                valueFunction = (ValueFunction)valueFunctionClass.newInstance();
            } catch (Exception e) {
                //logger.warn(valueFunctionCode); // composition time
                throw new SyntaxError(e.getMessage());
            }
		}
		
		else if (inputString.length() > 1 && inputString.charAt(0) == '@') {
		    indirectField = new Field(inputString.substring(1));
            type = ValueType.INDIRECT_FIELD;
            valueFunctionClass = null;
            valueFunctionCode = null;
            valueFunction = null;
		}

		else {
            indirectField = null;
            type = ValueType.PRIMITIVE;
            valueFunctionClass = null;
            valueFunctionCode = null;
            valueFunction = null;
		}
	}
	
	/**
	 * this constructor allow avoiding compilation per thread
	 * 
	 * @param valueFunction
	 * @param inputString
	 */
    public Value(String inputString, ValueFunction valueFunction, Class<?> valueFunctionClass, String code) {
	    this.inputString = inputString;
	    this.valueFunction = valueFunction;
	    this.valueFunctionCode = code;
        this.valueFunctionClass = valueFunctionClass;
	    this.indirectField = null;
        this.type = ValueType.DJK_JAVA;
	}
	
    private Value(String inputString, ValueType nonDjkJavaType) throws IOException {
        this.inputString = inputString;
        this.type = nonDjkJavaType;
        this.valueFunctionClass = null; 
        this.valueFunction = null; 
        this.valueFunctionCode = null; 
        indirectField = (inputString.length() > 1 && inputString.charAt(0) == '@') ?
                new Field(inputString.substring(1)) : null; 
    }
    
    /**
     * 
     * @return
     * @throws IOException
     */
    public Object replicate() throws IOException {
        if (valueFunctionClass != null) {
            ValueFunction function;
            try {
                function = (ValueFunction)valueFunctionClass.newInstance();
                return new Value(inputString, function, valueFunctionClass, valueFunctionCode);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IOException(e);
            }
        }
        
        return new Value(inputString, type);
    }

    /**
     * 
     * @param rec
     * @return
     * @throws Exception 
     */
	public Object getValue(Record rec) throws IOException {
	    Object value = null;

		if (valueFunction != null) {
		    try {
		        value = valueFunction.get(rec);
		    }
		    
		    // allow non-existent fields
		    catch (NullPointerException e) {
		        return null;
		    }
		    
		    catch (Exception e) {
                logger.error(valueFunctionCode); // run time
		        throw e;
		    }
		}
		
		else if (indirectField != null) {
		    indirectField.init(rec);
		    if (indirectField.next()) {
		        value = indirectField;
		    }
		    
		    else {
		        value = null;
		    }
		}

		else {
		    value = ReaderFormatParser.getMostSpecificPrimitive(inputString);
		}
		
	    return value;
	}
	
	public String getValueAsString(Record rec) throws IOException {
	    Object value = getValue(rec);
	    if (value == null) return null;
	    
	    if (value instanceof String) {
	        return (String)value;
	    }
	    
        if (value instanceof Boolean) {
            return Boolean.toString((Boolean)value);
        }

        if (value instanceof Long) {
            return Long.toString((Long)value);
        }
        
        if (value instanceof Double) {
            return Double.toString((Double)value);
        }
        
        if (value instanceof Field) {
            return ((Field)value).getValueAsString();
        }

        return null;
	}

	public ValueType getType() {
	    return type;
	}
	
    /**
     * 
     * @return a string suitable for display (removes djkJava curleys)
     */
    public String getDisplayString() {
        String is = inputString;
        return type == ValueType.DJK_JAVA ?
                // kill the semicolon too if there
                is.substring(1, is.length() - (is.endsWith(";}") ? 2 : 1)) :
                inputString;
    }
    
    @Override
    public String toString() {
        return getDisplayString();
    }
    
	public String getInputString() {
		return inputString;
	}

	@Description(topic="VALUE", text = { "The VALUE type uses a java-like syntax for accessing record field values.",
	"For the current record, the syntax 'x.ident' has the following meanings: "},
	preLines={
	        "P.ident refers to the System Property 'ident'. All System Properties are of type String.",
	        "R.ident refers to the record itself, and 'ident' refers to a method on the record. e.g. fieldsSupersetOf",
	        "",
	        "for the remaining cases:, 'x.ident' refers to the first instance of field 'ident' where:",
	        "* r. is of type Record (i.e. this is a sub-record field)",
	        "* s. is of type String",
	        "* l. is of type Long",
	        "* d. is of type Double",
	        "* b. is of type Boolean",
	        "",
	        "You can think of a VALUE expression as any java expression that can be assigned to the corresponding primative object.",
	        "The x.ident is an actual java variable with all methods of that type available, e.g.",
	        "s.text.matches(\"k.*\") is a regular expression match on the 'text' field.",
	        "",
	        "If the evaluation of a VALUE type throws a NPE, VALUE will result in null.",
	        "This allows for expressions to refer to fields that do not occur in every record. See 'add', 'acceptIf', 'rejectIf'",
	    }
	)

	@Example(expr = "myProperty=blue; [ id:1 ] add:color:'{P.myProperty;}'", type=ExampleType.EXECUTABLE)
	@Example(expr = "[ id:1,size:12 ] add:euSize:'{l.size * 4;}'", type=ExampleType.EXECUTABLE)
	@Example(expr = "[ noun:cats noun:dog ] add:isPlural:'{s.noun.endsWith(\"s\");}'", type=ExampleType.EXECUTABLE)
	@Example(expr = "[ id:1,size:3,color:blue,title:hat ] add:productOk:'{R.fieldsSupersetOf(\"id,title\");}'", type=ExampleType.EXECUTABLE)
	public static class Entry extends ManPage {
		public Entry() {
			super("VALUE");
		}
	}
}
