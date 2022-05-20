package com.amazon.djk.expression;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * jdk expressions occur in 3 basic contexts:
 * 
 * 1) from the command line where the shell splits the whitespace separated input into arg strings(aka 'chunks',
 * shellArgs) The user may need to supply single-quotes (ticks) to keep whitespace containing tokens together. 
 * Ticks (or escape) are also required to keep the shell from interpreting certain characters: *,&,etc. 
 * Such ticks disappear and are not available to define white-space containing strings within 'slots'   
 * 
 * 2) As a monolithic String.  The motivating case for these is in example annotations which are displayed and
 * run in man pages. The goal for the man page is to provide cut-n-paste examples that can be run at the 
 * command line.  Don't forget the ticks around elements that require them, e.g. '*', '&'.  Parsing in this
 * context must deal with the ticks properly.
 * 
 * 3) As String[] of operator tokens generally within unit tests.  Here the shell tokenizing has been done by
 * the programmer.  The String[] elements are equivalent to the main(args) of 1).
 * 
 * With regards to parsing there is:
 * 1) parsing String to operator arg strings (either via the shell, or shellTokenize())
 * 2) parsing 'chunks' into 'slots'.
 * 
 * Slots are delimited by colons and commas.  The first slot of a chunk must be delimited by
 * a colon or end-of-string (e.g. chunk=head:100, slot1=head, slot2=100). subsequent
 * slots may be delimited with commas (e.g. rm:id,title,size or add:foo:bar,goo:woo).
 * 
 * If the first char of a slot is { and the last char is } then everything between the curlies
 * (exclusive) is interpretted as "djk-java", i.e the right hand side of a variable java 
 * assignment, e.g. {"here are colon's and commas :,:,:,:";} is interpretted as a string.
 * Furthermore, no slot delimiting characters are recognized therein.  This is the only and
 * best method to assign strings with arbitrary characters into a record field from the
 * command line.  (Programmatically there are simpler ways, of course).
 * 
 * Below are some example strings that are parsable:
 * 
 * "head:200 add:foo:'{l.id * 450;}',goo:bar devnull"
 * "rm:id,title,size"
 * "tokCount=countToks:title"
 * 
 */
public abstract class SlotTokenizer {
    
    /**
     * 
     */
    public enum SlotType {OPNAME_POSITION, ARG, PARAM, OPTIONAL_ARG};
    public enum SlotEnd {NONE, COMMA, COLON, AMPERSAND, EQUALS}; 
    public static class ParseSlot {
        public final SlotType type;
        public final String string;
        public final SlotEnd end;
        
        public ParseSlot(String string, SlotType type, SlotEnd end) {
            this.string = string;
            this.type = type;
            this.end = end;
        }
        
        public String toString() {
            return type.toString() + ' ' +  string + ' ' + end.toString(); 
        }
        
        public char getEndAsChar() {
            switch (end) {
            case NONE:
                return '\n';
                
            case COMMA:
                return ',';
                
            case COLON:
                return ':';

            case AMPERSAND:
                return '&';
                
            case EQUALS:
                return '=';
                
            default:
                return 0;
            }
        }
    }

    /**
     * tokenize an operator chunk into slots for an operator
     * 
     * @param inputChunk
     * @return
     * @throws SyntaxError 
     */
    public static List<ParseSlot> split(String inputChunk) {
        List<ParseSlot> slots = new ArrayList<>();
        
        // take care of parameters
        List<ParseSlot> paramSlots = new ArrayList<>();
        int parmsPos = addParameterSlots(inputChunk, paramSlots);
        if (parmsPos != -1) {
            // remove from input
            inputChunk = inputChunk.substring(0, parmsPos);
        }
        
        // treat 'foo=op:bar' and 'foo,goo,boo=oper:bar' as a special cases
        // allow one slot only on the left hand side of an equals
        // valid fields = ^[-A-Za-z0-9_.]+$ (see KnifeDefs) 
        Pattern preEqualsPattern = Pattern.compile("^(?<preEquals>[-A-Za-z0-9_.]+)\\=(?<postEquals>.*)");
        Matcher m = preEqualsPattern.matcher(inputChunk);
        if (m.find()) {
            String preEquals = m.group("preEquals");
            slots.add(new ParseSlot(preEquals, SlotType.ARG, SlotEnd.EQUALS));
            inputChunk = m.group("postEquals");
        }

        // tokenize on colon and comma
        List<String> tokens = tokenizeByCommaAndColon(inputChunk);
        
        int numSlots = 0;
        for (int i = 0; i < tokens.size(); i++) {
            String tok = tokens.get(i);
            char delim = lastChar(tok);
            tok = removeLastChar(tok);

            SlotType type = numSlots == 0 ? SlotType.OPNAME_POSITION : SlotType.ARG; 
            
            SlotEnd slotEnd = delim == ':' ? SlotEnd.COLON :
                delim == ',' ? SlotEnd.COMMA : SlotEnd.NONE;
            
            slots.add(new ParseSlot(tok, type, slotEnd));
            numSlots++;
        }

        // the last token can optionally be an OPTIONAL arg
        // with the comaAndColon parsing, this is spread over
        // the last and penultimate token.
        // 1) penultimate ends in [
        // 2) ultimate ends in ]

        int num = slots.size();
        if (num > 1) {
            ParseSlot pen = slots.get(num-2);
            ParseSlot ult = slots.get(num-1);
            if (lastChar(pen.string) == '[' && lastChar(ult.string) == ']') {
                slots.set(num-2, new ParseSlot(removeLastChar(pen.string), pen.type, pen.end));
                slots.set(num-1, new ParseSlot(removeLastChar(ult.string), SlotType.OPTIONAL_ARG, SlotEnd.NONE));
            }
        }
        
        slots.addAll(paramSlots);
        
        return slots;
    }
    
    /**
     * add parameter slots, where a parameters slot holds one name value pair
     * 
     * @param input string to parse
     * @param slots the output slots
     * @return the character offset of the parameters within the input or -1 if non-existent
     */
    private static int addParameterSlots(String input, List<ParseSlot> slots) {
    	int qmark = findParameters(input);
    	if (qmark == -1) return -1;
    	
    	String paramsAsString = input.substring(qmark+1);
        
        // this won't catch every case
        Pattern validPair = Pattern.compile("[A-Za-z]+=.*");

        // tokenize by ampersand and see if they are well formed
        ArrayList<String> tokens = tokenizeByAmpersand(paramsAsString);
        // first verify every ok
        for (String tok : tokens) {
            char delim = lastChar(tok);
            if (delim != 0 && delim != '&') return -1;
            Matcher m = validPair.matcher(tok);
            if (!m.find()) return -1;
        }

        for (String tok : tokens) {
            char delim = lastChar(tok);
            tok = removeLastChar(tok); // remove delim
            slots.add(new ParseSlot(tok, SlotType.PARAM, delim == '&' ? SlotEnd.AMPERSAND : SlotEnd.NONE));
        }

        return qmark;
    }
    
    /**
     * We are overly restrictive here until we find a real need for the more complicated parsing.
     * valid parameters on a chunk look like this:
     * 
     * \\?[A-Za-z]+\\=[^&]? repeat
     *  
     * @param chunk
     * @return
     */
    private static int findParameters(String chunk) {
    	int qmark = chunk.lastIndexOf('?');  // must be the last one.
    	
    	int len = chunk.length();
    	
    	// makes sure the name-value matches exhaust the end of the chunk
    	Pattern nvPat = Pattern.compile("(?<name>[A-Za-z_0-9\\.]+)\\=(?<value>[^&]+)");
    	
    	Matcher m = nvPat.matcher(chunk);
    	int start = qmark + 1;
    	while (m.find(start)) {
    		if (m.start() != start) return -1;
    		String value = m.group("value");

    		// validate the value
    		if (!isValidValue(value)) return -1;
    		
    		start = m.end();
    		if (start == len) break;
			if (chunk.charAt(start) == '&') {
				start++;
			}
    	}
    	if (start != chunk.length()) return -1;
    	
    	return qmark;
    }
    
    private static boolean isValidValue(String value) {
    	if (value.startsWith("'") && !value.endsWith("'")) {
    		return false;
    	}
    	
    	if (!value.startsWith("'") && value.endsWith("'")) {
    		return false;
    	}
    	
    	if (value.startsWith("{") && !value.endsWith("}")) {
    		return false;
    	}
    	
    	if (!value.startsWith("{") && value.endsWith("}")) {
    		return false;
    	}
    	
    	return true;
    	
    }
    
    private static String removeLastChar(String input) {
        return input.substring(0, input.length()-1);
    }
    
    private static char lastChar(String s) {
        int last = s.length() - 1;
        return s.charAt(last);
    }
    
    private static ArrayList<String> tokenizeByQuestionMark(String input) {
        return tokenizeWithDelimiter(input, new char[]{'?', 0});
    }
    
    private static ArrayList<String> tokenizeByAmpersand(String input) {
        return tokenizeWithDelimiter(input, new char[]{'&', 0});
    }
    
    /**
     * tokenize by colon and comma but sensitive to not do so within curlies.
     * Note: one exception to colon breaks is the schema colon in ://
     * 
     * @param input string
     * @return a list of tokens with the break character as the last char
     */
    private static ArrayList<String> tokenizeByCommaAndColon(String input) {
        ArrayList<String> tokens = tokenizeWithDelimiter(input, new char[]{',', ':', 0});
        return tokens;
    }

    /**
     * 
     * @param input string
     * @param delimiters to break on
     * @return list of strings with delimiters as last char
     */
    private static ArrayList<String> tokenizeWithDelimiter(String input, char[] delimiters) {
        input += ((char)0); // for easier parsing add a zero

        ArrayList<String> tokens = new ArrayList<>();
        boolean insideCurley = false;
        
        // inside curley happens when charAt(leftEdge) == '{'
        
        int leftEdge = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            
            if (c == '{' && i == leftEdge) {
                insideCurley = true;
                continue;
            }

            if (c == '}') {
                insideCurley = false;
                continue;
            }
            
            // if not inside curley or if last char
            if (!insideCurley || c == 0) {
                for (char delim : delimiters) {
                    if (delim == c) {
                        tokens.add(input.substring(leftEdge, i + 1));
                        leftEdge = i + 1;
                        break;
                    }
                }
            }
        }
        
        return tokens;
    }
}
