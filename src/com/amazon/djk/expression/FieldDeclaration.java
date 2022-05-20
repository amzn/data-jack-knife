package com.amazon.djk.expression;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.ManPage;

public class FieldDeclaration {
	private static final Logger LOG = LoggerFactory.getLogger(FieldDeclaration.class);
    private static final Pattern FIELD_DEFINE_PATTERN = Pattern.compile("^ephemeral\\s+(?<type>(STRING|DOUBLE|LONG|BOOLEAN))\\s+(?<field>[^\\s=]+)\\s*=\\s*(?<value>[^\\s;]+);\\s*");
    private static final Pattern FIELD_DECLARE_PATTERN = Pattern.compile("^(?<type>(STRING|DOUBLE|LONG|BOOLEAN))\\s+(?<field>[^\\s;]+);\\s*");
	
    public final boolean isEphemeral;
    public final String type;
    public final String field;
    public final String value;
    
    public FieldDeclaration(boolean isEphemeral, String type, String field, String value) {
    	this.isEphemeral = isEphemeral;
    	this.type = type;
    	this.field = field;
    	this.value = value;
    }
    
	public static FieldDeclaration create(String chunk) throws SyntaxError {
		// must come before DECLARE test
        Matcher m = FIELD_DEFINE_PATTERN.matcher(chunk);
        if (m.matches()) {
            String type = m.group("type");
            String field = m.group("field");
            String value = m.group("value");
            return new FieldDeclaration(true, type, field, value);
        }
        
        m = FIELD_DECLARE_PATTERN.matcher(chunk);
        if (m.matches()) {
            String type = m.group("type");
            String field = m.group("field");
            return new FieldDeclaration(false, type, field, null);
        }
        
        return null;
	}
	
	@Override
	public String toString() {
		return isEphemeral ? String.format("ephemeral %s %s=%s", type, field, value) :
			String.format("%s %s", type, field);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof FieldDeclaration)) {
			return false;
		}

		FieldDeclaration fieldDeclaration = (FieldDeclaration) obj;

		return this.isEphemeral == fieldDeclaration.isEphemeral &&
				this.field.equals(fieldDeclaration.field) &&
				this.type.equals(fieldDeclaration.type) &&
				(this.value != null ? this.value.equals(fieldDeclaration.value) : fieldDeclaration.value == null); // value is optional
	}

	/**
	 * 
	 */
	@Description(topic="Declarations", text = { "DJK is schema-less but you can declare fields to be a specific type. ",
	"The native data sources (nat and natdb) contain records with explicit field types. However, text based sources ",
	"(nv2, tsv and inline records) are interpretted using a most-specific-type approach.  That means DJK attempts to ",
	"interpret a field first as Double, then a Long, finally as a String.  Field declarations make it possible to ",
	"declare the type of a field upfront. Additionally, the 'ephemeral' keyword defines a global field of a specific ",
	"type, with a specific value.  Ephemeral fields are more efficient than regular fields.  Like their name implies, ",
	"ephemeral fields are not persisted by a sink. Ephemerals must be defined to an initial value."},
	preLines={
	        "Field declarations, like Properties, must be declared before the expression, e.g:",
	        "STRING myString;",
	        "LONG myLong;",
	        "DOUBLE myDouble;",
	        "BOOLEAN myBoolean;",
	        "",
	        "the 'empemeral' keyword can be prepended to any of these, e.g:",
	        "ephemeral BOOLEAN myBoolean = false;"
	    }
	)

	@Example(expr = "[ num:1,token:2.3 ]", type=ExampleType.EXECUTABLE)
	@Example(expr = "STRING token; [ num:1,token:2.3 ]", type=ExampleType.EXECUTABLE)
	@Example(expr = "ephemeral LONG id=4; [ num:1 ] [ add:foo:bar if:'{l.id == 4;}' add:id:3 [ add:hello:world if:'{l.id == 3;}'", type=ExampleType.EXECUTABLE)
	public static class Entry extends ManPage {
		public Entry() {
			super("Declarations");
		}
	}
}
