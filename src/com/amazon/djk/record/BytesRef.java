package com.amazon.djk.record;

/**
 * object for copying bytes from another buffer
 *
 */
public class BytesRef implements Comparable<BytesRef> {
    public static final int LONG_SIZE = 8;
    protected static final int SHORT_SIZE = 2;
    protected static final int INT_SIZE = 4;
	protected byte[] bytes;
	protected int offset;
	protected int length;
	protected int lastNumVarLenBytes = 0;

	public BytesRef() { 
		this.bytes = null;
		this.offset = 0;
		this.length = 0;
	}
	
	protected void set(BytesRef ref) {
		this.bytes = ref.bytes;
		this.offset = ref.offset;
		this.length = ref.length;
	}
	
    protected void set(byte[] bytes, int offset, int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }
	
	/**
	 * 
	 * @return the number of bytes holding record information
	 */
	public int size() {
		return length;
	}
	
	/**
	 * 
	 * @return the total capacity of this record in bytes.
	 */
	public int capacity() {
		return bytes.length;
	}

	/**
	 * expose the underlying buffer of this BytesRef
	 */
	public byte[] buffer() {
		return bytes;
	}

    /**
     * 
     * @param off
     * @return
     */
    protected byte getByteAt(int off) {
        return bytes[off];
    }
    
    /**
     * 
     * @param off
     * @return
     */
    protected boolean getBooleanAt(int off) {
        return bytes[off] == 1;
    }
	
	 /**
	  *  
	  *@param  off  a starting offset into the byte array
	  *@return         the short (16-bit) value
	  */
	protected short getShortAt(int off) {
        int b1 = bytes[off] & 0xFF;
        int b0 = bytes[off+1] & 0xFF;
        return (short) ((b1 << 8) + (b0 << 0));
    }
	
	/**
     *  
     *@param  off  a starting offset into the byte array
     *@return         the int (32-bit) value
     */
    protected int getIntAt(int off) {
        int i=off;
        int b3 = bytes[i++] & 0xFF;
        int b2 = bytes[i++] & 0xFF;
        int b1 = bytes[i++] & 0xFF;
        int b0 = bytes[i++] & 0xFF;
        return (b3 << 24) + (b2 << 16) + (b1 << 8) + (b0 << 0);
    }
	
    /**
     *  get a long value from a byte array
     *
     *@param  data    the byte array
     *@param  off  a starting offset into the byte array
     *@return         the long (64-bit) value
     */
    protected long getLongAt(int off) {
        long result = 0;
        
        for (int j = off + LONG_SIZE - 1; j >= off; j--) {
            result <<= 8;
            result |= 0xff & bytes[j];
        }
        return result;
    }
	
    /**
     *  get a double value from a byte array, reads it in little endian format
     *  then converts the resulting revolting IEEE 754 (curse them) floating
     *  point number to a happy java double
     *
     *@param  offset  a starting offset into the byte array
     *@return         the double (64-bit) value
     */
    protected double getDoubleAt(int offset) {
        return Double.longBitsToDouble(getLongAt(offset));
    }

    protected int getLastNumVarLenBytes() {
        return lastNumVarLenBytes;
    }
    
    protected long getVarLenUnsignedLongAt(int offset) {
        long value = 0L;
        int i = 0;
        long b;
        lastNumVarLenBytes = 1;
        while (((b = bytes[offset++]) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
            
            lastNumVarLenBytes++;
        }

        return value | (b << i);
    }

    protected long getVarLenSignedLongAt(int offset) {
        long raw = getVarLenUnsignedLongAt(offset);
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }
    
    /**
     * 
     * @param offset
     * @return
     */
    protected int getVarLenUnsignedIntAt(int offset) {
        byte b = getByteAt(offset);
        
        // one byte
        if ((b & 0xC0) == 0) {
            lastNumVarLenBytes = 1;
            return b;
        }
        
        // two bytes
        if ((b & 0xC0) == 64) {
            lastNumVarLenBytes = 2;
            return getShortAt(offset) & 0x3FFF;
        }
        
        // else four bytes
        lastNumVarLenBytes = 4;
        return getIntAt(offset) & 0x7FFFFFFF;
    }
    
    public int offset() {
        return offset;
    }
    
    public int length() {
        return length;
    }

    @Override
    public int compareTo(BytesRef o2) {
        int offset2 = o2.offset();
        int len2 = o2.length();
        byte[] bytes2 = o2.buffer();
        
        int lim = Math.min(length, len2);
        
        int i = 0;
        while (i < lim) {
            byte a = bytes[offset + i];
            byte b = bytes2[offset2 + i];
            if (a != b) {
                return (a & 0xFF) - (b & 0xFF);
            }

            i++;
        }
        
        return length - len2;
    }
    
    public boolean contains(BytesRef r2) {
        return indexOf(r2) != -1;
    }
    
    /**
     * based on String.indexOf.  Return the index into this bytes array, where the
     * bytes sequence of o2 begins or -1 if o2 is not present in this bytes array.
     * 
     * @param o2
     * @return
     */
    public int indexOf(BytesRef o2) {
        // o2 is the target, 'this' is the source 
        int targetOffset = o2.offset(); 
        int targetCount = o2.length();
        byte[] target = o2.buffer();

        int sourceCount = length;
        int sourceOffset = offset;

        if (targetCount == 0) {
            return 0;
        }
            
        byte first  = target[targetOffset];
        int max = sourceOffset + (sourceCount - targetCount);
            
        for (int i = sourceOffset; i <= max; i++) {
            /* Look for first character. */
            if (bytes[i] != first) {
                while (++i <= max && bytes[i] != first);
            }
            
            /* Found first character, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = targetOffset + 1; j < end && bytes[j] ==
                        target[k]; j++, k++);
            
                if (j == end) {
                    /* Found whole string. */
                    return i - sourceOffset;
                }
            }
        }
        
        return -1;
    }
}
