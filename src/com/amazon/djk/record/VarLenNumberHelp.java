package com.amazon.djk.record;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Helper methods in sync with Bytes/BytesRef 
 *
 */
public class VarLenNumberHelp {

    /**
     * encodes integers as either 1,2 or 4 bytes.
     * 
     * @param value
     * @throws IOException 
     */
    public static void writeVarLenUnsignedInt(DataOutputStream stream, int value) throws IOException {
        // 0-63 => one byte [00xxxxxx]
        if (value < 64) {
            stream.write((byte)((value >>> 0) & 0x3F));
            return;
        }
        
        // 64-16383 => two bytes [01xxxxxx] [xxxxxxxx]
        if (value < 16384 ) {
            stream.write((byte)(((value >>> 8) & 0x3F) | 0x40));
            stream.write((byte)((value >>> 0) & 0xFF));
            return;
        } 

        // else int => four bytes [1xxxxxxx] ...
        stream.write((byte)(((value >>> 24) & 0x7F) | 0x80));
        stream.write((byte)((value >>> 16) & 0xFF));
        stream.write((byte)((value >>> 8) & 0xFF));
        stream.write((byte)((value >>> 0) & 0xFF));
    }
    
    /**
     * static method to make it possible to read varInt from ByteBuffer.
     * buf.position() after the call will reflect the len of the var len.
     * 
     * @param buf
     * @param offset
     * @return
     */
    public static int getVarLenUnsignedInt(ByteBuffer buf) {
        byte b = buf.get();
        
        // one byte
        if ((b & 0xC0) == 0) {
            return b;
        }
        
        // two bytes
        if ((b & 0xC0) == 64) {
            buf.position(buf.position()-1);
            return buf.getShort() & 0x3FFF;
        }
        
        // else four bytes
        buf.position(buf.position()-1);
        return buf.getInt() & 0x7FFFFFFF;        
    }
    
    /**
     * static method to make it possible to read from ByteBuffer.
     * buf.position() after the call will reflect the len of the var len.
     * @param buf
     * @param offset
     * @return
     */
    public static long getVarLenUnsignedLong(ByteBuffer buf) {
        long value = 0L;
        int i = 0;
        long b;
        
        int offset = buf.position();
        
        int lastNumVarLenBytes = 1;
        while (((b = buf.get()) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
            
            lastNumVarLenBytes++;
        }

        buf.position(offset + lastNumVarLenBytes);

        return value | (b << i);
    }
    
    public static long getVarLenSignedLongAt(ByteBuffer buf) {
        long raw = getVarLenUnsignedLong(buf);
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }
}
