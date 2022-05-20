package com.amazon.djk.pipe;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.*;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.Gloss;
import com.amazon.djk.record.Field;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.UTF8BytesRef;
import com.amazon.djk.report.ReportFormats;
import com.amazon.djk.report.ScalarProgress;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;

@ReportFormats(headerFormat="needle=%s haystack=%s minMatch=%s")
public class TextMatchPipe extends RecordPipe {
    private static final String IS_MATCH_FIELD = "isMatch";
    private static final String DEFAULT_MIN_MATCH = "100%";
    private static final String MIN_MATCH = "minMatch";
    private static final String DEFAULT_ADD_TYPE_PARAM = "false";
    private static final String ADD_TYPE = "addType";
    private static final String HAYSTACK = "HAYSTACK";
    private static final String NEEDLE = "NEEDLE";
    private static final String MATCH_TYPE = "matchType";
    private final OpArgs args;

	@ScalarProgress(name="needle")
	private final Field needle;
	@ScalarProgress(name="haystack")
	private final Field haystack;
	@ScalarProgress(name="minMatch")
    private final String minMatchSpec;
	
    private final UTF8BytesRef haystackRef = new UTF8BytesRef();
    private final UTF8BytesRef haystackTokRef = new UTF8BytesRef();
    private final UTF8BytesRef needleRef = new UTF8BytesRef();
    private final UTF8BytesRef needleTokRef = new UTF8BytesRef();

    private final boolean addType;
	private final MinMatch minMatch;
	
	private final Field explicitType;
	private final boolean requireExplicitType;
    
	enum MatchType {NONE, BROAD, PHRASE, EXACT};
	
    public TextMatchPipe(OpArgs args) throws IOException {
        this(null, args);
    }
    
    public TextMatchPipe(TextMatchPipe root, OpArgs args) throws IOException {
    	super(root);
    	this.args = args;
		this.needle = (Field)args.getArg(NEEDLE);
		this.haystack = (Field)args.getArg(HAYSTACK);
        this.minMatchSpec = (String)args.getParam(MIN_MATCH);

        // the argument for the minMatch Object. default to exhaustive match
        // for cases of broad|phrase|exact|@minMatchField
        String minMatchArg = "100%";
        String matchTypeField = null;
        
        switch (minMatchSpec) {
        case "phrase":
        case "broad":
        case "exact":
        	requireExplicitType = true;
        	break;
        	
        default:
        	// case of @matchTypeField
        	if (minMatchSpec.charAt(0) == '@') {
        		matchTypeField = minMatchSpec.substring(1);
            	requireExplicitType = true;
        	}
        	
        	else {
        		requireExplicitType = false;
        		minMatchArg = minMatchSpec;
        	}
        }
        
        // object creation
        minMatch = new MinMatch(minMatchArg);
        this.addType = (Boolean)args.getParam(ADD_TYPE) && minMatchSpec.equals("100%");
        
        // if non-null, means the matchType required comes from the record itself
        explicitType = matchTypeField != null ? new Field(matchTypeField) : null;
    }

    /**
     * This MinMatch class has been copied from AmazonClicksSearchCommons
     * This implementation replaces the earlier DJK implementation of MinMatch
     * since the old implementation was erroneous for cases like 2,100%
     * Unit tests from AmzonClicksSearchCommons have also been copied over
     *
     * a minMatchSpec is an expression that describes the minimum
     * number of matches a document must have in order to succeed.
     * Typically this is used in a query context to define a match
     * somewhere between and AND and an OR query.  But it can also
     * be used to define the minimum number of matches the keyword
     * vetting context.  It has the following format:
     *
     * minMatchSpec = mm1[,mm2,mm3,mm4,...]
     *
     * where mm1 = minimum number of matches given length = 1
     *       mm2 = minimum number of matches given length = 2, etc.
     *       and the last specified length is valid from then on.
     *
     * alternatively it can be specified as
     *
     * minMatchSpec = X%
     *
     * where the min match is always = X% of length
     * or
     * minMatchSpec = mm1,mm2,X%
     * which implies that for any input of length >= 3, required match is X%
     *
     */
    public static class MinMatch {
        String[] parts;

        /**
         *
         * @param minMatchSpec - see header
         */
        public MinMatch(String minMatchSpec) {
            if (StringUtils.isBlank(minMatchSpec)) {
                parts = new String[1];
                parts[0] = "100%"; //null spec means 100% match
                return;
            }

            if (minMatchSpec.indexOf(',') == -1) {
                parts = new String[1];
                parts[0] = minMatchSpec;
            } else {
                parts = minMatchSpec.split(",");
            }
        }

        /**
         *
         * @param inputLength - either number of query terms, or for vetting, keyword terms
         * @return minMatch
         */
        public int getMinMustMatch(int inputLength) {
            if (inputLength == 0) return 0;
            int offset = Math.min(inputLength-1, parts.length-1);
            String minMatchSpec = parts[offset];
            if (minMatchSpec.endsWith("%")) {
                minMatchSpec = minMatchSpec.substring(0,minMatchSpec.length()-1);
                float percent = Float.parseFloat(minMatchSpec) / 100.0F;
                return (int)((float)inputLength * percent);
            } else {
                return Integer.parseInt(minMatchSpec);
            }
        }

    }
    
    @Override
	public Object subReplicate() throws IOException {
    	return new TextMatchPipe(this, args);
    }

    @Override
	public Object replicate() throws IOException {
    	return new TextMatchPipe(this, args);
    }

    @Override
    public Record next() throws IOException {
    	Record rec = super.next();
    	if (rec == null) return null;

        if (!rec.getFirstAsUTF8BytesRef(needle, needleRef)) {
            return rec; 
        }
        
        int hitCount = 0;
        int needleTermCount = 0;
        
        needleRef.whitespaceTokenizeInit();
        while (needleRef.nextWhitespaceToken(needleTokRef)) {
            needleTermCount++;
                        
            if (!rec.getFirstAsUTF8BytesRef(haystack, haystackRef)) {
                return rec; 
            }
            
            haystackRef.whitespaceTokenizeInit();
            while (haystackRef.nextWhitespaceToken(haystackTokRef)) {
                if (needleTokRef.compareTo(haystackTokRef) == 0) {
                    hitCount++;
                    break;
                }
            }
        }
 
    	boolean isMatch = hitCount >= minMatch.getMinMustMatch(needleTermCount);
    	rec.deleteAll(IS_MATCH_FIELD);
    	
    	if (requireExplicitType) {
    		evaluateExplicitType(isMatch, rec);
    	}
    	
    	else {
    		rec.addField(IS_MATCH_FIELD, isMatch);
        	if (addType) addTypeField(isMatch, rec);        	
    	}
    	
    	return rec;
    }

    /**
     * 
     * @param isMatch
     * @param rec
     * @return the match type that occurred
     */
    private MatchType getMatchType(boolean isMatch, Record rec) {
    	if (!isMatch) {
    		return MatchType.NONE;
    	}
    	
    	rec.getFirstAsUTF8BytesRef(haystack, haystackRef);
		if (needleRef.compareTo(haystackRef) == 0) {
			return MatchType.EXACT;
		}
		
		else if (haystackRef.indexOf(needleRef) != -1) {
			return MatchType.PHRASE;
		}
		
		else {
			return MatchType.BROAD;
		}    	
    }

    /**
     * evaluate the match that occurred against the explicitly required match type
     * 
     * @param isMatch
     * @param rec
     * @throws IOException
     */
    private void evaluateExplicitType(boolean isMatch, Record rec) throws IOException {
    	MatchType occurredType = getMatchType(isMatch, rec);
    	switch (occurredType) {
    	case BROAD:
        	String requiredType = explicitType != null ? rec.getFirstAsString(explicitType) : minMatchSpec;
    		rec.addField(IS_MATCH_FIELD, requiredType.equals("broad")); 
    		break;
    		
    	case PHRASE:
        	requiredType = explicitType != null ? rec.getFirstAsString(explicitType) : minMatchSpec;
    		rec.addField(IS_MATCH_FIELD, requiredType.equals("phrase") || requiredType.equals("broad")); 
    		break;
    		
    	case EXACT:
    		// regardless of explicit type
    		rec.addField(IS_MATCH_FIELD, true);
    		break;
    	
    	case NONE:
    	default:
    		// regardless of explicit type
    		rec.addField(IS_MATCH_FIELD, false);
    	}
    }    	

    /**
     * add the the type of match that occurred to the record
     * 
     * @param isMatch whether a match occurred a per the minMatch object
     * @throws IOException 
     */
    private void addTypeField(boolean isMatch, Record rec) throws IOException {
    	MatchType occurredType = getMatchType(isMatch, rec);
    	switch (occurredType) {
    	case BROAD:
    		rec.addField(MATCH_TYPE, "broad");
    		break;
    		
    	case PHRASE:
    		rec.addField(MATCH_TYPE, "phrase");
    		break;
    		
    	case EXACT:
    		rec.addField(MATCH_TYPE, "exact");
    		break;
    	
    	case NONE:
    	default:
    		rec.addField(MATCH_TYPE, "none");	
    	}
    }
    
    /**
     * 
     * @return
     */
    @Description(text={"Determines the degree to which NEEDLE words are a subset of HAYSTACK words.",
            "Updates the field 'isMatch' to true or false appropriately (See working example). Not the same implementation as solr minMatch."
    })
    @Arg(name= NEEDLE, gloss="The whitespace tokenized subset field.", type=ArgType.FIELD)
    @Arg(name= HAYSTACK, gloss="The whitespace tokenized superset field.", type=ArgType.FIELD)
    @Param(name=MIN_MATCH, gloss="Either SPEC or TYPE. Adds field isMatch=true|false", type=ArgType.STRING, eg="1,2,2,3", defaultValue = DEFAULT_MIN_MATCH)
    @Param(name=ADD_TYPE, gloss="If true and minMatch=100% will add the field matchType=broad|phrase|exact", type=ArgType.BOOLEAN, defaultValue = DEFAULT_ADD_TYPE_PARAM)
    @Gloss(entry="SPEC", def="the solr-like match specification, e.g. 100% or 1,2,2,3")
    @Gloss(entry="TYPE", def="defines the required matchType=broad|phrase|exact|@matchTypeField")
    
    // this param is costly
    //@Param(name="addCounts", gloss="optional. If true adds 'needleTermCount', 'haystackTermCount' and 'hitCount' fields. default=false.", type=ArgType.BOOLEAN, eg="false")
    @Example(expr="[ kw:'a b c',query:'a b e z w' ] txtmatch:kw:query", type=ExampleType.EXECUTABLE)
    @Example(expr="[ kw:'a b c',query:'a b e z w' ] txtmatch:kw:query'?'minMatch=1,2,2,3", type=ExampleType.EXECUTABLE)
    @Example(expr="[ kw:'a b',query:'x a b e' ] txtmatch:kw:query'?'addType=true", type=ExampleType.EXECUTABLE)
    @Example(expr="[ kw:'a b',query:'x a b e' ] txtmatch:kw:query'?'minMatch=phrase", type=ExampleType.EXECUTABLE)    
    @Example(expr="[ kw:'a b',query:'x a b e',matchType:broad ] txtmatch:kw:query'?'minMatch=@matchType", type=ExampleType.EXECUTABLE)        
    public static class Op extends PipeOperator {
    	public Op() {
        	super("txtmatch:NEEDLE:HAYSTACK");
		}

        @Override
        public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
            return new TextMatchPipe(args).addSource(operands.pop());
        }
    }
}
