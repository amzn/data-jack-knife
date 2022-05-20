package com.amazon.djk.record;

import java.io.IOException;

public class UTF8BytesRef extends BytesRef {
    private int tokenStart = -2;
    private int tokenEnd = -2;

    /**
     * points to the reference to the incoming bytes known to be a UTF8 representation.
     * @param bytes
     */
    public void set(Bytes bytes) {
        this.bytes = bytes.bytes;
        this.offset = bytes.offset;
        this.length = bytes.length;
    }
    
    public String getAsString() throws IOException {
        return ThreadDefs.get().getUTF8BytesRefAsString(this);
    }
    
    public void whitespaceTokenizeInit() {
        tokenStart = offset;
        tokenEnd = offset - 1;
    }
    
    public boolean nextWhitespaceToken(UTF8BytesRef out) {
        if (tokenEnd == -2) return false; // not inited
        
        // find first non-white after end
        int i = tokenEnd + 1;
        for (; i < offset + length; i++) {
            if (!Character.isWhitespace(bytes[i])) {
                break;
            }
        }
        
        if (i == offset + length) return false;
        out.bytes = bytes;        
        tokenStart = i;
        for (i = tokenStart + 1; i <= offset + length + 1; i++) {
            if (i == offset + length || Character.isWhitespace(bytes[i])) {
                out.offset = tokenStart;
                tokenEnd = i;
                out.length = tokenEnd - tokenStart;
                return true;
            }
        }
        
        return false;
    }
}
