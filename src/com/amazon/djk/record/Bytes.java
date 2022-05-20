package com.amazon.djk.record;

/**
 * object for copying bytes from another buffer
 *
 */
public class Bytes extends BytesRef {

	public Bytes() {
		bytes = new byte[32];
		length = 0;
		offset = 0;
	}

	// Resize buffer to accommodate new bytes.
	protected void resize(int add) {
		if (length + add > bytes.length) {
			int newlen = bytes.length < MODERATE ? 
					(bytes.length * 3) / 2 + 1 : // a la ArrayList 
					bytes.length + add + 1024;
			if (length + add > newlen) {
				newlen = length + add;
			}
			byte[] newbuf = new byte[newlen];
			System.arraycopy(bytes, 0, newbuf, 0, length);
			bytes = newbuf;
		}
	}

	private final static int MODERATE = 1024 * 1024;
	private final static int HUGE = 1024 * 1024 * 10;
	public void reset() {
		length = 0;
		offset = 0;
		if (bytes.length > HUGE) {
			bytes = new byte[0];
		}
	}
	
	/**
	 * put one byte into this Bytes
	 * 
	 * @param b
	 */
	protected void putByte(byte b) {
        resize (1);
        bytes[length++] = b;
	}
	
    /**
     *  put a short value into this Bytes
     *
     *@param  offset  a starting offset into the byte array
     *@param  value   the short (16-bit) value
     */
    protected void putShort(short value) {
        resize(2);
        bytes[length++] = (byte)((value >>>  8) & 0xFF);
        bytes[length++] = (byte)((value >>>  0) & 0xFF);
    }
    
	/**
     *  put a short value into this Bytes
     *
     *@param  offset  a starting offset into the byte array
     *@param  value   the short (16-bit) value
     */
    protected void putShortAt(int offset, short value) {
        resize(2);
        bytes[offset++] = (byte)((value >>>  8) & 0xFF);
        bytes[offset++] = (byte)((value >>>  0) & 0xFF);
    }
	
    /**
     *  put an int value into this Bytes
     *
     *@param  value   the int (32-bit) value
     */
    protected void putInt(int value) {
        resize(4);
        bytes[length++] = (byte)((value >>> 24) & 0xFF);
        bytes[length++] = (byte)((value >>> 16) & 0xFF);
        bytes[length++] = (byte)((value >>>  8) & 0xFF);
        bytes[length++] = (byte)((value >>>  0) & 0xFF);
    }

    /**
     *  put an int value into this Bytes
     *
     *@param  value   the int (32-bit) value
     */
    protected void putIntAt(int offset, int value) {
        bytes[offset++] = (byte)((value >>> 24) & 0xFF);
        bytes[offset++] = (byte)((value >>> 16) & 0xFF);
        bytes[offset++] = (byte)((value >>>  8) & 0xFF);
        bytes[offset++] = (byte)((value >>>  0) & 0xFF);
    }

    /**
     *  put a long value into this Bytes
     *
     *@param  offset  a starting offset into the byte array
     *@param  value   the long (64-bit) value
     */
    protected void putLong(long value) {
        resize(LONG_SIZE);
        int limit = LONG_SIZE + length;
        long v = value;
        
        for (int j = length; j < limit; j++) {
            bytes[j] = (byte) (v & 0xFF);
            v >>= 8;
        }
        
        length = limit;
    }
    
    /**
     *  put a long value into this Bytes
     *
     *@param  offset  a starting offset into the byte array
     *@param  value   the long (64-bit) value
     */
    protected void putLongAt(int offset, long value) {
        int limit = LONG_SIZE + offset;
        long v = value;
        
        for (int j = offset; j < limit; j++) {
            bytes[j] = (byte) (v & 0xFF);
            v >>= 8;
        }
    }
    
    /**
     *  put a double value into this Bytes
     *
     *@param  offset  a starting offset into the byte array
     *@param  value   the double (64-bit) value
     */
    protected void putDouble(double value) {
        putLong(Double.doubleToLongBits(value));
    }
    
    /**
     *  put a double value into this Bytes
     *
     *@param  off  a starting offset into the byte array
     *@param  value   the double (64-bit) value
     */
    protected void putDoubleAt(int off, double value) {
        putLongAt(off, Double.doubleToLongBits(value));
    }
    
    /**
     * put the boolean value into this Bytes
     * 
     * @param offset
     * @param value
     */
    protected void putBoolean(boolean value) {
        resize (1);
        bytes[length++] = (value ? (byte)1 : (byte)0);
    }

    /**
     * put the boolean value into this Bytes at the given offset
     * 
     * @param off
     * @param value
     */
    protected void putBooleanAt(int off, boolean value) {
        resize (1);
        bytes[off++] = (value ? (byte)1 : (byte)0);
    }
    
    /**
     * 
     * @param value
     */
    protected void putVarLenUnsignedLong(long value) {
        resize(10); // why not 9?  causes outofbounds
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            bytes[length++] = (byte)(((int) value & 0x7F) | 0x80);
            value >>>= 7;
        }
        bytes[length++] = (byte)((int) value & 0x7F);
    }
    
    /**
     * 
     * @param value
     */
    protected void putVarLenSignedLong(long value) {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        putVarLenUnsignedLong((value << 1) ^ (value >> 63));
    }
    
    protected void putVarLenDouble(double value) {
        putVarLenSignedLong(Double.doubleToLongBits(value));
    }
    
    /**
     * encodes integers as either 1,2 or 4 bytes.
     * 
     * @param value
     */
    protected void putVarLenUnsignedInt(int value) {
        resize(4);
        // 0-63 => one byte [00xxxxxx]
        if (value < 64) {
            bytes[length++] = (byte)((value >>> 0) & 0x3F);
            return;
        }
        
        // 64-16383 => two bytes [01xxxxxx] [xxxxxxxx]
        if (value < 16384 ) {
            bytes[length++] = (byte)(((value >>> 8) & 0x3F) | 0x40);
            bytes[length++] = (byte)((value >>> 0) & 0xFF);
            return;
        } 

        // else int => four bytes [1xxxxxxx] ...
        bytes[length++] = (byte)(((value >>> 24) & 0x7F) | 0x80);
        bytes[length++] = (byte)((value >>> 16) & 0xFF);
        bytes[length++] = (byte)((value >>> 8) & 0xFF);
        bytes[length++] = (byte)((value >>> 0) & 0xFF);
    }
    
	/**
	 * put the bytes array into this Bytes
	 * 
	 * @param buffer
	 * @param off
	 * @param add
	 */
    protected void putBytes(byte[] buffer, int off, int add) {
		// If ADD < 0 then arraycopy will throw the appropriate error for us
		if (add >= 0) resize (add);
        System.arraycopy(buffer, off, bytes, length, add);
		length += add;
	}
    
    /**
     * put the bytes referenced by ref into this Bytes
     * 
     * @param ref
     */
    protected void putBytes(BytesRef ref) {
        if (ref.length >= 0) resize(ref.length);
        System.arraycopy(ref.bytes, ref.offset, bytes, length, ref.length);
        length += ref.length;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof Bytes) ) {
            return false;
        }
        
        Bytes r = (Bytes) obj;
        if (r.length != length) return false;

        byte[] otherBytes = r.buffer();
        for (int i = 0; i < length; i++) {
            if (bytes[i] != otherBytes[i]) {
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public int hashCode() {
        int code = 1;
        for (int i = 0; i < length; i++) {
            code = 31 * code + bytes[i];
        }

        return code;
    }
    
    public byte[] getAsByteArray() {
        byte[] ret = new byte[length];
        System.arraycopy(bytes, 0, ret, 0, length);
        return ret;
    }
}
