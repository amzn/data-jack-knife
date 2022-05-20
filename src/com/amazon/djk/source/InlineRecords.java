package com.amazon.djk.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.MinimalRecordSource;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.core.Splittable;
import com.amazon.djk.expression.ChunkTokenizer;
import com.amazon.djk.expression.ExpressionChunks;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParseToken;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.expression.SlotTokenizer.ParseSlot;
import com.amazon.djk.expression.SlotTokenizer.SlotEnd;
import com.amazon.djk.expression.SlotTokenizer.SlotType;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.expression.TokenResolver;
import com.amazon.djk.format.ReaderFormatParser;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.FieldType;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.Value;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;

/**
 * 
 * This source allows you to start a create a single record source
 * inlined in djk expression. e.g. djk rec:title:'blue shoes',brand:adidas ...
 * it's equivalent to:
 * 
 * djk [ foo:bar,boo:[s1:v1,s2:v2] ]
 * djk [ foo:bar boo:woo,sub:[foo:bar],sub:[boo:woo] zoo:doo ]
 *
 */
@ReportFormats(lineFormats={"<expression>%s"})
public class InlineRecords extends MinimalRecordSource implements Splittable {
    public static final String RIGHT_SCOPE = "]";
    private final List<ParseToken> tokens;
	@ScalarProgress(name="expression")
    private final String expression;

	/**
	 * 
	 * @param inlineExpression in form '[ hello:world,id:2 hello:there,id:1 ]'
	 * @return
	 * @throws SyntaxError
	 * @throws IOException
	 */
	public static InlineRecords create(String inlineExpression) throws SyntaxError, IOException {
		String[] chunks = ChunkTokenizer.split(inlineExpression);
		List<ParseToken> toks = TokenResolver.getTokens(new ExpressionChunks(chunks));
		if (toks.size() < 3) {
			throw new SyntaxError("improper inline syntax"); 
		}
		
		ParseToken t = toks.remove(0);
		if (!t.toString().equals("[")) {
			throw new SyntaxError("improper inline syntax"); 
		}
		
		t = toks.remove(toks.size()-1);
		if (!t.toString().equals("]")) {
			throw new SyntaxError("improper inline syntax"); 
		}
		
		return new InlineRecords(toks);
	}
	
    private InlineRecords(List<ParseToken> tokens) throws SyntaxError, IOException {
        this.tokens = tokens;
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (int i = 0; i < tokens.size(); i++) {
            ParseToken token = tokens.get(i); // get the correct order
            sb.append(token.getString());
            sb.append(" ");
            
            // parse the records here to see if they throw syntax error (i..e. pass in null)
            getAsRecord(token, null);
            
        }
        sb.append("]");
        
        expression = sb.toString();
    }
    
    private InlineRecords(ParseToken token) {
        tokens = new Stack<>();
        tokens.add(token);
        expression = "";
    }
    
    @Override
    public Record next() throws IOException {
        if (tokens.isEmpty()) return null;
        ParseToken token = tokens.remove(tokens.size()-1);
        try {
            Record out = new Record();
            getAsRecord(token, out);
            return out;
        } catch (SyntaxError e) {
            throw new IOException(e);
        }
    }
    
    @Override
    public Object split() throws IOException {
        if (tokens.size() < 2) return null;
        return new InlineRecords(tokens.remove(tokens.size()-1));
    }

    /**
     * Primarily for testing. Provides a single line representation of a record.  Will break 
     * when String fields contain [ or ] or comma, therefore primarily for testing.
     * 
     * @param rec the Record
     * @return a single line specification of the record
     * @throws IOException 
     */
    public static String getAsSingleLine(Record rec) throws IOException {
        StringBuilder sb = new StringBuilder();
        getAsSingleLine(rec, sb);
        return sb.toString();
    }
    
    /**
     * builds a single line representation of a record (li)mited to not contain [],)
     * @param rec
     * @param out
     * @throws IOException 
     */
    private static void getAsSingleLine(Record rec, StringBuilder out) throws IOException {
        FieldIterator fields = new FieldIterator();

        fields.init(rec);
        while (fields.next()) {
            out.append(fields.getName());
            out.append(':');
            if (fields.getType() == FieldType.RECORD) {
                Record subrec = new Record();
                fields.getValueAsRecord(subrec);
                out.append('[');
                getAsSingleLine(subrec, out);
                out.append(']');
            }

            else {
                String v = fields.getValueAsString();
                if (v.indexOf(']') != -1 ||
                    v.indexOf('[') != -1 ||
                    v.indexOf(',') != -1) {
                    throw new SyntaxError("cannot construct single line record when string fields contain {'[',']',','}");
                }

                out.append(fields.getValueAsString());
            }
            
            out.append(',');
        }
        
        // remove last comma
        out.setLength(out.length()-1);
    }
    
    /**
     * retokenize the slots of a single record which came in one chunk from the shell or shell tokenizer.
     * 
     * special tokenization for inline records which turn [,] into slots
     * 
     * e.g. goo:woo,sub:[one:two,sub2:[three:four]],foo:bar
     * goo:
     * woo,
     * sub:
     * [
     * one:
     * two,
     * sub2:
     * [
     * three:
     * four
     * ]
     * ]
     * foo:
     * bar
     * ]  # top level record final marker
     * 
     * 
     * @param slots
     * @return
     * @throws SyntaxError 
     */
    private static List<ParseSlot> retokenize(List<ParseSlot> slots) throws SyntaxError {
        // need more extensive checking here.
        if (slots.size() < 2) throw new SyntaxError("improper inline record syntax");
        List<ParseSlot> out = new ArrayList<>();
        for (ParseSlot slot : slots) {
            String ss = slot.string;
            
            if (ss.startsWith("[")) {
                out.add(new ParseSlot("[", SlotType.ARG, SlotEnd.NONE));
                out.add(new ParseSlot(ss.substring(1), slot.type, slot.end));
            }

            else if (ss.endsWith("]")) {
                int end = ss.indexOf(']');
                out.add(new ParseSlot(ss.substring(0, end), slot.type, slot.end));
                for (int i = end; i < ss.length(); i++) {
                    // must be only ] for rest of string
                    if (ss.charAt(i) != ']') throw new SyntaxError("improper inline record syntax");
                    out.add(new ParseSlot("]", SlotType.ARG, SlotEnd.NONE));
                }
            }
            
            else {
                out.add(slot);
            }
        }
        
        // top level record final marker
        out.add(new ParseSlot("]", SlotType.ARG, SlotEnd.NONE));
        
        return out;
    }
    
    private static void getAsRecord(ParseToken token, Record out) throws SyntaxError, IOException {
        List<ParseSlot> slots = token.getSlots();
        slots = retokenize(slots);
        getAsRecord(token, slots.iterator(), out);
    }
    
    /**
     * parse the retokenized slots
     * 
     * @param token
     * @param iter
     * @param out if null, just parse and throw a SyntaxError as appropriate
     * @throws SyntaxError
     * @throws IOException
     */
    private static void getAsRecord(ParseToken token, Iterator<ParseSlot> iter, Record out) throws SyntaxError, IOException {
        //ParseSlot v;

        while (iter.hasNext()) {
            ParseSlot n = iter.next();
            String name = n.string;
            if (name.equals("]")) { // end of record
                return;
            }            
            
            ParseSlot v = iter.hasNext() ? iter.next() : null;
            if (v == null || v.end == SlotEnd.COLON) {
                throw new SyntaxError("error in inline record --> " + token.getString());
            }
            
            String valueAsString = v.string;
            if (valueAsString.equals("[")) { // beginning subrecord
                if (out != null) {
                    Record s = new Record();
                    getAsRecord(token, iter, s);
                    out.addField(name, s);
                } else {
                    getAsRecord(token, iter, null);
                }
            }
            
            else if (out != null){
                Value value = new Value(valueAsString);
                if (value.getType() == Value.ValueType.PRIMITIVE) {
                    
                    ReaderFormatParser.addPrimitiveValue(out, name, value.getInputString());
                }
                
                else { // else we are a function of 'this', i.e., the record
                    out.addField(new Field(name), value);
                }
            }
        }
    }
    
	public static RecordSource create(List<ParseToken> tokens) throws SyntaxError, IOException {
		// be tolerant of the stack either including or not including the enclosing '[' and ']' tokens
        ParseToken begin = tokens.get(0);
        if (begin.getString().equals("[")) {
            tokens.remove(0);
        }
        
        int endPos = tokens.size() - 1;
        ParseToken end = tokens.get(endPos);
        if (end.getString().equals("]")) {
            tokens.remove(endPos);
        }
		
        // currently the decision is to not allow suppression of source reports
        return new InlineRecords(tokens);
	}
	
	/**
	 * never actually used by the parser since there is custom code for inline records.
	 * purpose here is to provide a man page. 
	 *
	 */
	@Description(
            text={"The right scope delimiter used to define an inline record source. Spaces delimit records, commas delimit name-value pairs within a record.  Supports djk-java syntax for VALUES."},
            contexts={"[ REC_SPEC ...]"})
	@Gloss(entry="REC_SPEC", def="NAME:VALUE,NAME:VALUE ...")
	@Gloss(entry="NAME", def="name of the field.")
	@Gloss(entry="VALUE", def="value of the field. e.g. 23 or {d.price * 1.5;} See 'VALUE' man page.")
    public static class Op extends SourceOperator {
		public final static String RIGHT_SCOPE = "]";
    	public Op() {
			super(RIGHT_SCOPE, Type.NAME);
		}

    	@Override
    	public RecordSource getSource(OpArgs args) throws IOException, SyntaxError {
    		return null; // never instantiated
    	}
    }
}
