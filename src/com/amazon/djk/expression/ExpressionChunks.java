package com.amazon.djk.expression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.amazon.djk.processor.KnifeProperties;
import com.amazon.djk.processor.KnifeProperties.Namespace;

/**
 * improve readability 
 *
 */
public class ExpressionChunks implements Iterable<String>{
	private final List<String> chunks = new ArrayList<>();

	public ExpressionChunks() { }
	
	public ExpressionChunks(String[] input) {
		add(input);
	}
	
	public ExpressionChunks(String input) {
		add(ChunkTokenizer.split(input));
	}
	
	public void add(String chunk) {
		chunks.add(chunk);
	}
	
	public void add(String[] chunks) {
		for (String chunk : chunks) {
			this.chunks.add(chunk);
		}
	}
	
	public int size() {
		return chunks.size();
	}
	
	public String getAsString() {
		return StringUtils.join(chunks, " ");
	}
	
	public String[] toArray() {
		return (String[])chunks.toArray(new String[0]);
	}
	
	public boolean isEmpty() {
		return chunks.isEmpty();
	}
	
	public void set(int index, String chunk) {
		chunks.set(index, chunk);
	}
	
	public String get(int index) {
		return chunks.get(index);
	}
	
	public void remove(int index) {
		chunks.remove(index);
	}
	
	@Override
	public Iterator<String> iterator() {
		return chunks.iterator();
	}

	/*
	 * resolve the job properties within the expression chunks using the previously set KnifeProperties.
	 */
	public void resolveProperties(Namespace propertiesNamespace) throws SyntaxError {
		for (int i = 0; i < chunks.size(); i++) {
			String chunk = KnifeProperties.resolveProperties(propertiesNamespace, chunks.get(i));
			chunks.set(i, chunk);
		}		
	}
	
	@Override
	public String toString() {
		return chunks.toString();
	}
}
