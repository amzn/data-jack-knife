package com.amazon.djk.report;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.amazon.djk.record.Record;

/**
 * in the following format example:
 * recsRead, recsRate, recsPercent and files are expected to be annotated names on RecordSource fields
 * 
 * "recsRead=%d (<recsRate>%2.1frecs/sec <recsPercent>%2.1f%%) files=%d"
 * 
 * 'recsRead' is matched as 'b4equals'
 * the others are matched as 'toinsert'
 */
public class ProgressFormat {
	private final static Pattern annotatedPattern = Pattern.compile("(?<b4equals>\\w+)\\s*=\\s*%|(?<toinsert>\\<[^>]+\\>)%");
	private final ConcurrentHashMap<String,ScalarResolver> resolverMap;
	private final String outputFormat;
	private final String[] annotatedNames; // existing in the format
	private final Object[] outputVars;
	private final int maxLineLength;

	/**
	 * 
	 * @param annotation
	 * @param resolverMap
	 * @param maxLineLength
	 */
	public ProgressFormat(String annotation, ConcurrentHashMap<String,ScalarResolver> resolverMap, int maxLineLength) {
		this.resolverMap = resolverMap;
		this.maxLineLength = maxLineLength;
		StringBuilder sb = new StringBuilder();
		Matcher m = annotatedPattern.matcher(annotation);
		
		int curr = 0;
		List<String> tempNames = new ArrayList<>();
		while (m.find()) {
			String b4equals = m.group("b4equals");
			if (b4equals != null) {
				tempNames.add(b4equals);
				sb.append(annotation.substring(curr, m.end()));
				curr = m.end();
			}
			
			else {
				String toinsert = m.group("toinsert");
				if (toinsert != null) {
					tempNames.add(toinsert.substring(1, toinsert.length()-1)); // substring out the <>'s
					sb.append(annotation.substring(curr, m.start()));
					curr = m.end() - 1; // -1 because of the %
				}
			}
		}
		
		sb.append(annotation.substring(curr, annotation.length()));
		outputVars = new Object[tempNames.size()];
		outputFormat = sb.toString();
		annotatedNames = tempNames.toArray(new String[0]);
	}
	
	@Override
	public String toString() {
		return outputFormat + ", " + StringUtils.join(annotatedNames, ",");
	}

	/**
	 * 
	 * @param resolvers
	 * @return
	 */
	public List<String> resolve() {
		for (int i = 0; i < annotatedNames.length; i++) {
			ScalarResolver resolver = resolverMap.get(annotatedNames[i]);
			if (resolver == null) {
				throw new RuntimeException("annoted name '" + annotatedNames[i] + "' in format does not exist on a field");
			}
			
 			outputVars[i] = resolver.getValue();
		}
		
		String formattedLine = String.format(outputFormat, outputVars);
		List<String> linez = new ArrayList<>();
		
		// If line has \n's respect them, else chop up by maxline
		String[] parts = formattedLine.split("\n");
		for (String p : parts) {
			// allows skipping a '%s' format with null value, note the compare to "null"
			if (!p.equals("null") || p.length() == 0) {
				linez.add(p);
			}
		}
		
		int i = 0;
		while (i < linez.size()) {
			String line = linez.get(i);
			if (line.length() < maxLineLength) {
				i++;
				continue;
			}
			
			line = linez.remove(i);
			while (line.length() > maxLineLength) {
				String beg = line.substring(0, maxLineLength);
				linez.add(i++, beg);
				line = line.substring(maxLineLength);
			}

			if (line.length() != 0) {
				linez.add(i, line);
			}
			
			i++;
		}
		
		return linez;
	}
	
	/**
	 * Writes the annotated variables contained in this format into target
	 * however, it uses resolver 'StableName' instead of the annotated one
	 * it also uses the resolver's toString() so that it can provide a
	 * default format
	 *  
	 * @param target into which the variables are written
	 * @throws IOException 
	 */
	public void addTo(Record target) throws IOException {
		for (String annotation : annotatedNames) {
			ScalarResolver resolver = resolverMap.get(annotation);
			Object obj = resolver.getValue();
			
			if (obj instanceof Integer) {
				long val = (Integer)obj;
				target.addField(resolver.getStableName(), val);
			}
			
			else if (obj instanceof Long) {
				long val = (Long)obj;
				target.addField(resolver.getStableName(), val);
			}
			
			else if (obj instanceof Float) {
				double val = (Float)obj;
				target.addField(resolver.getStableName(), val);
			}
			
			else if (obj instanceof Double) {
				double val = (Double)obj;
				target.addField(resolver.getStableName(), val);				
			}
			
			else {
				target.addField(resolver.getStableName(), resolver.toString());
			}
		}
	}
}
