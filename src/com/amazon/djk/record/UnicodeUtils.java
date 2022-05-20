package com.amazon.djk.record;

import com.amazon.djk.format.FormatException;

public class UnicodeUtils {
	/**
	 * 
	 */
	public static final int UNI_SUR_HIGH_START = 0xD800;
	public static final int UNI_SUR_HIGH_END = 0xDBFF;
	public static final int UNI_SUR_LOW_START = 0xDC00;
	public static final int UNI_SUR_LOW_END = 0xDFFF;
	public static final int UNI_REPLACEMENT_CHAR = 0xFFFD;

	private static final long UNI_MAX_BMP = 0x0000FFFF;

	private static final long HALF_SHIFT = 10;
	private static final long HALF_MASK = 0x3FFL;

	private static final int SURROGATE_OFFSET = Character.MIN_SUPPLEMENTARY_CODE_POINT
			- (UNI_SUR_HIGH_START << HALF_SHIFT) - UNI_SUR_LOW_START;

	/** Maximum number of UTF8 bytes per UTF16 character. */
	public static final int MAX_UTF8_BYTES_PER_CHAR = 4;
	
	public static void putUTF8Bytes(final CharSequence s, Bytes out) {
		final int slen = s.length();
		out.resize(slen * MAX_UTF8_BYTES_PER_CHAR);
		byte[] bytes = out.buffer();
		int length = out.length;
		
		for (int i = 0; i < slen; i++) {
			final int code = (int) s.charAt(i);

			if (code < 0x80)
				bytes[length++] = (byte) code;
			else if (code < 0x800) {
				bytes[length++] = (byte) (0xC0 | (code >> 6));
				bytes[length++] = (byte) (0x80 | (code & 0x3F));
			} else if (code < 0xD800 || code > 0xDFFF) {
				bytes[length++] = (byte) (0xE0 | (code >> 12));
				bytes[length++] = (byte) (0x80 | ((code >> 6) & 0x3F));
				bytes[length++] = (byte) (0x80 | (code & 0x3F));
			} else {
				// surrogate pair
				// confirm valid high surrogate
				if (code < 0xDC00 && (i < slen - 1)) {
					int utf32 = (int) s.charAt(i + 1);
					// confirm valid low surrogate and write pair
					if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
						utf32 = (code << 10) + utf32 + SURROGATE_OFFSET;
						i++;
						bytes[length++] = (byte) (0xF0 | (utf32 >> 18));
						bytes[length++] = (byte) (0x80 | ((utf32 >> 12) & 0x3F));
						bytes[length++] = (byte) (0x80 | ((utf32 >> 6) & 0x3F));
						bytes[length++] = (byte) (0x80 | (utf32 & 0x3F));
						continue;
					}
				}
				// replace unpaired surrogate or out-of-order low surrogate
				// with substitution character
				bytes[length++] = (byte) 0xEF;
				bytes[length++] = (byte) 0xBF;
				bytes[length++] = (byte) 0xBD;
			}
		}
		
		out.length = length;
	}

	/**
	 * 
	 * @param bytes
	 * @param offset
	 * @param length
	 * @param out
	 * @return
	 *
	 * NOTE: Full characters are read, even if this reads past the length passed (and
	 * can result in an ArrayOutOfBoundsException if invalid UTF-8 is passed).
	 * Explicit checks for valid UTF-8 are not performed
	 */
	protected static void getUTF8BytesRefAsString(UTF8BytesRef input, StringBuilder out) throws FormatException {
		out.setLength(0);
		int off = input.offset;

		final int limit = off + input.length;
		while (off < limit) {
			int b = input.bytes[off++] & 0xff;
			if (b < 0xc0) {
				if (b >= 0x80) throw new FormatException("UTF encoding error");
				out.append((char) b);
			} else if (b < 0xe0) {
				out.append((char) (((b & 0x1f) << 6) + (input.bytes[off++] & 0x3f)));
			} else if (b < 0xf0) {
				out.append((char) (((b & 0xf) << 12)
						+ ((input.bytes[off] & 0x3f) << 6) + (input.bytes[off + 1] & 0x3f)));
				off += 2;
			} else {
				assert b < 0xf8 : "b = 0x" + Integer.toHexString(b);
				int ch = ((b & 0x7) << 18) + ((input.bytes[off] & 0x3f) << 12)
						+ ((input.bytes[off + 1] & 0x3f) << 6)
						+ (input.bytes[off + 2] & 0x3f);
				off += 3;
				if (ch < UNI_MAX_BMP) {
					out.append((char) ch);
				} else {
					int chHalf = ch - 0x0010000;
					out.append((char) ((chHalf >> 10) + 0xD800));
					out.append((char) ((chHalf & HALF_MASK) + 0xDC00));
				}
			}
		}
	}
}
