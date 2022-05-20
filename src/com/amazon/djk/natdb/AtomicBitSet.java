package com.amazon.djk.natdb;

import java.util.concurrent.atomic.AtomicLong;

/**
 * pieces stolen from java bitset impl. 
 *
 */
public class AtomicBitSet {
	  /*
     * BitSets are packed into arrays of "words."  Currently a word is
     * a long, which consists of 64 bits, requiring 6 address bits.
     * The choice of word size is determined purely by performance concerns.
     */
    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private final static int BIT_INDEX_MASK = BITS_PER_WORD - 1;

    /* Used to shift left or right for a partial word mask */
    private static final long WORD_MASK = 0xffffffffffffffffL;
    
	private final AtomicLong[] words;
	
	/**
     * Given a bit index, return word index containing it.
     */
    private static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }
	
    public class SetBitIterator {
    	int currBitIndex = 0;
    	
    	public int next() {
    		int next = AtomicBitSet.this.nextSetBit(currBitIndex);
    		currBitIndex = next + 1;
    		return next;
    	}
    }
    
    public class ClearBitIterator {
    	int currBitIndex = 0;
    	
    	public int next() {
    		int next = AtomicBitSet.this.nextClearBit(currBitIndex);
    		currBitIndex = next + 1;
    		return next;
    	}
    }
    
    public SetBitIterator setBitIterator() {
    	return new SetBitIterator();
    }
    
    public ClearBitIterator clearBitIterator() {
    	return new ClearBitIterator();
    }
    
    /**
     * 
     * @param numBits
     */
	public AtomicBitSet(int numBits) {
		int numLongs = 1 + numBits / 64;
		words = new AtomicLong[numLongs];
		for (int i = 0; i < numLongs; i++) {
			words[i] = new AtomicLong(0);
		}
	}
	
	/**
	 * 
	 * @param bitIndex
	 */
	public void set(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int wordIndex = wordIndex(bitIndex);
		AtomicLong cell = words[wordIndex];
		long mask = 1L << bitIndex;
		 
		long expect;
		long update;
		 
		do {
			expect = cell.get(); 
			update = expect | mask;
		} while(!cell.compareAndSet(expect, update));
	}
	 
	/**
	 * 
	 * @param bitIndex
	 * @return
	 */
	public boolean get(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int wordIndex = wordIndex(bitIndex);
		if (wordIndex > words.length) return false;
		
		long mask = 1L << bitIndex;
		AtomicLong cell = words[wordIndex]; 
		
		return (mask & cell.get()) != 0;
	}
	
	/**
	 * 
	 * @param fromIndex
	 * @return
	 */
	private int nextSetBit(int fromIndex) {
		if (fromIndex < 0)
			throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

		int u = wordIndex(fromIndex);
		if (u >= words.length)
			return -1;

		long word = words[u].get() & (WORD_MASK << fromIndex);

		 while (true) {
			 if (word != 0)
				 return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
			 if (++u == words.length)
				 return -1;
			 word = words[u].get();
		 }
	 }
	
	/**
     * Returns the index of the first bit that is set to {@code false}
     * that occurs on or after the specified starting index.
     *
     * @param  fromIndex the index to start checking from (inclusive)
     * @return the index of the next clear bit
     * @throws IndexOutOfBoundsException if the specified index is negative
     * @since  1.4
     */
    public int nextClearBit(int fromIndex) {
        // Neither spec nor implementation handle bitsets of maximal length.
        // See 4816253.
        if (fromIndex < 0)
            throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

        int u = wordIndex(fromIndex);
        if (u >= words.length)
            return fromIndex;

        long word = ~(words[u].get()) & (WORD_MASK << fromIndex);

        while (true) {
            if (word != 0)
                return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            if (++u == words.length)
                return words.length * BITS_PER_WORD;
            word = ~(words[u].get());
        }
    }
}
