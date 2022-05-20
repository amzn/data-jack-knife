package com.amazon.djk.pipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.Param;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.format.ReaderFormatParser;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Record;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
/**
 * Pipe for creating fields from another field via named-group regex.
 * 
 * NOTE: parameters are impossible to include with this pipe due to
 * the fact that regex's likely contain question-marks, the delimiter to parameters.
 * It would be possible if parameters were encoded, but that isn't an option  
 * since this is a command line tool.
 *
 */
@ReportFormats(headerFormat="input=%s regex=%s")
public class AddRegexFieldsPipe extends RecordPipe {
    @ScalarProgress(name="regex")
	private final String regex;
    @ScalarProgress(name="input")
    private Field input;
	private final OpArgs args;
	private final Pattern pattern;
	private final String childName;
	private final List<String> matchNames;
	private final Record child = new Record();
	
    public AddRegexFieldsPipe(OpArgs args) throws IOException {
        this(null, args);
    }
	
    /**
     * replica constructor 
     * @param root
     * @param args
     * @throws IOException
     */
    private AddRegexFieldsPipe(AddRegexFieldsPipe root, OpArgs args) throws IOException {
        super(root);
        this.args = args;
        input = (Field)args.getArg("INPUT");
        regex = (String)args.getArg("REGEX");
        pattern = Pattern.compile(regex);
        matchNames = getRegexGroupNames(regex);

        // if ends$ or starts^ only matches ONCE
        childName = (regex.endsWith("$") || regex.startsWith("^")) ? 
        		null : "child"; 
    }
	
    @Override
    public Object replicate() throws IOException {
        return new AddRegexFieldsPipe(this, args);
    }
    
    @Override
    public Record next() throws IOException {
        Record rec = super.next();
        if (rec == null) return null;
        
        input.init(rec);
        while (input.next()) {
        	String value = input.getValueAsString();
        	addRegex(rec, value);
        }
        
        return rec;
    }
    
    /**
     * Adds field values per regex matches.
     * 
     * @param out record to add matches to
     * @param value the field value to match against
     * @throws IOException
     */
    private void addRegex(Record out, String value) throws IOException {
        Matcher mm = pattern.matcher(value);
        if (!mm.find()) return;

        // no subrecords, add fields to main record
        if (childName == null) {
        	for (String name : matchNames) {
        		String v = mm.group(name);
                ReaderFormatParser.addPrimitiveValue(out, name, v);
            }
        } 
        
        else {
        	do {
        		child.reset();
        		for (String name : matchNames) {
        			String v = mm.group(name);
        			ReaderFormatParser.addPrimitiveValue(child, name, v);
        		}
            
        		out.addField(childName, child);
        	} while (mm.find());
        }
    }
    
    /**
     * 
     * @param regexWithNamedGroups
     * @return
     * @throws SyntaxError
     */
    private static List<String> getRegexGroupNames(String regexWithNamedGroups) throws SyntaxError {
    	List<String> names = new ArrayList<>();
    	
        // grab the field names out of the regex
        Pattern valueFieldsPattern = Pattern.compile("(\\<[^\\>]+)\\>");
        Matcher mm = valueFieldsPattern.matcher(regexWithNamedGroups);
        while (mm.find()) {
            String temp = mm.group();
            names.add(temp.substring(1, temp.length()-1));  // get rid of <>
        }
        if (names.isEmpty()) {
            throw new SyntaxError(regexWithNamedGroups + " contains no named groups cooresponding to fields, e.g. (?<color>.*)");
        }
        
        return names;
    }
    
    @Override
    public boolean reset() {
    	return true; // nothing to do
    }

    @Description(text={"Adds fields based on the named-groups in REGEX run over the INPUT field.",
    			       "If the REGEX begins with '^' or ends with '$' then no child (sub-records)",
    			       "are produced.  Otherwise, all REGEX matches will be added as 'child' ",
    			       "sub-records."})

    @Arg(name="INPUT", gloss="Field over which REGEX is run.", type=ArgType.FIELD, eg="text")
    @Arg(name="REGEX", gloss="name value pairs of fields to add", type=ArgType.STRING, eg="c=(?<color>\\w+)")
    @Example(expr="[ color:blue,text:'{\"0@@B0032,1:B0012,5:B0015,8@@5690\";}' ] regex:text:'(\\d+@@)?(?<id>[^,]+),(?<score>[^:@]+):?'", type=ExampleType.EXECUTABLE)
    @Example(expr="[ color:red,text:'{\"0@@B0032,1:B0012,5:B0015,8@@5690\";}' ] regex:text:'^(\\d+@@)?(?<id>[^,]+),(?<score>[^:@]+):?'", type=ExampleType.EXECUTABLE)    public static class Op extends PipeOperator {
    	public Op() {
    		super("regex:INPUT:REGEX");
    	}
        	
    	@Override
    	public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
    		return new AddRegexFieldsPipe(args).addSource(operands.pop());
    	}
    }
}
