package com.amazon.djk.record;

/**
 * Fields and SubRecords in a record are of the form:
 * where (N) means N bytes
 * 
 * fid(2) | type(1) | payload(N)
 *   
 * This class is a reference to the type + payload, i.e. everything but the field id.
 * 
 * E.g.
 * 
 * for type = {STRING,RECORD}
 * type(1) | payloadLen(4) | payload(payloadLen)
 * i.e. length = 5 + payloadLen
 * 
 * for type = {LONG,DOUBLE}
 * type(1) | payload(8)
 * i.e. length = 9
 * 
 * for type = {BOOLEAN}
 * type(1) | payload(1)
 * i.e. length = 2
 * 
 * for type = {NULL}
 * type(1)
 * i.e. length = 1
 * 
 */
public class FieldBytesRef extends BytesRef {
    public FieldType getType() {
        return length < 1 ? FieldType.ERROR :
            FieldType.getType(bytes[0]);
    }
}
