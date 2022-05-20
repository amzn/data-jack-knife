package com.amazon.djk.misc;

import com.amazon.djk.record.BytesRef;

public class Hashing {
    public static final int SEED = 0;
    
    public static long hash64(BytesRef bytes) {
        return hash64(bytes, SEED);
    }

    public static long hash64(BytesRef bytes, int seed) {
        return hash64(bytes.buffer(), bytes.offset(), bytes.length(), seed);
    }

    public static long hash64(byte[] bytes, int offset, int length) { 
        return hash64(bytes, offset, length, SEED);
    }
    
    public static long hash63(BytesRef bytes) {
        return hash63(bytes, SEED);
    }

    public static long hash63(BytesRef bytes, int seed) {
        return hash63(bytes.buffer(), bytes.offset(), bytes.length(), seed);
    }

    public static long hash63(byte[] bytes, int offset, int length, int seed) {
        return hash64(bytes, offset, length, seed) & 0x7FFFFFFFFFFFFFFFL;
    }
    
    public static long hash64(byte[] bytes, int offset, int length, int seed) {        
        byte[] data = bytes;
        
        final int blockCount = length >> 4;
        long h1 = seed;
        long h2 = seed;
        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        for (int i = 0; i < blockCount; i++) {
            long k1 =
                    (data[offset] & 0xFFL)
                            | ((data[offset + 1] & 0xFFL) << 8)
                            | ((data[offset + 2] & 0xFFL) << 16)
                            | ((data[offset + 3] & 0xFFL) << 24) |
                            ((data[offset + 4] & 0xFFL) << 32)
                            | ((data[offset + 5] & 0xFFL) << 40)
                            | ((data[offset + 6] & 0xFFL) << 48)
                            | ((data[offset + 7] & 0xFFL) << 56);
            offset += 8;
            long k2 =
                    (data[offset] & 0xFFL)
                            | ((data[offset + 1] & 0xFFL) << 8)
                            | ((data[offset + 2] & 0xFFL) << 16)
                            | ((data[offset + 3] & 0xFFL) << 24) |
                            ((data[offset + 4] & 0xFFL) << 32)
                            | ((data[offset + 5] & 0xFFL) << 40)
                            | ((data[offset + 6] & 0xFFL) << 48)
                            | ((data[offset + 7] & 0xFFL) << 56);
            offset += 8;

            k1 *= c1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        long k1 = 0;
        long k2 = 0;
        switch (length & 15) {
        case 15:
            k2 ^= (data[offset + 14] & 0xFFL) << 48;
        case 14:
            k2 ^= (data[offset + 13] & 0xFFL) << 40;
        case 13:
            k2 ^= (data[offset + 12] & 0xFFL) << 32;
        case 12:
            k2 ^= (data[offset + 11] & 0xFFL) << 24;
        case 11:
            k2 ^= (data[offset + 10] & 0xFFL) << 16;
        case 10:
            k2 ^= (data[offset + 9] & 0xFFL) << 8;
        case 9:
            k2 ^= (data[offset + 8] & 0xFFL) << 0;
            k2 *= c2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= c1;
            h2 ^= k2;

        case 8:
            k1 ^= (data[offset + 7] & 0xFFL) << 56;
        case 7:
            k1 ^= (data[offset + 6] & 0xFFL) << 48;
        case 6:
            k1 ^= (data[offset + 5] & 0xFFL) << 40;
        case 5:
            k1 ^= (data[offset + 4] & 0xFFL) << 32;
        case 4:
            k1 ^= (data[offset + 3] & 0xFFL) << 24;
        case 3:
            k1 ^= (data[offset + 2] & 0xFFL) << 16;
        case 2:
            k1 ^= (data[offset + 1] & 0xFFL) << 8;
        case 1:
            k1 ^= (data[offset + 0] & 0xFFL);
            k1 *= c1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= c2;
            h1 ^= k1;
        }

        h1 ^= length;
        h2 ^= length;
        h1 += h2;
        h2 += h1;

        // fmix h1:
        h1 ^= h1 >>> 33;
        h1 *= 0xff51afd7ed558ccdL;
        h1 ^= h1 >>> 33;
        h1 *= 0xc4ceb9fe1a85ec53L;
        h1 ^= h1 >>> 33;

        // fmix h2:
        h2 ^= h2 >>> 33;
        h2 *= 0xff51afd7ed558ccdL;
        h2 ^= h2 >>> 33;
        h2 *= 0xc4ceb9fe1a85ec53L;
        h2 ^= h2 >>> 33;

        h1 += h2;
        h2 += h1;

        return h1;
    }
}