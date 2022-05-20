package com.amazon.djk.record;

public enum FieldType {
	STRING(0), 
	LONG(1), 
	DOUBLE(2),
	BOOLEAN(3),
	RECORD(4),
    NULL(5),
    BYTES(6),
	ERROR(7);
	
	private final int value;
    public static final int STRING_ID = 0;
    public static final int LONG_ID = 1;
    public static final int DOUBLE_ID = 2;
    public static final int BOOLEAN_ID = 3;
    public static final int RECORD_ID = 4;
    public static final int NULL_ID = 5;
    public static final int BYTES_ID = 6;
    public static final int ERROR_ID = 7;
	
	private FieldType(int value) {
		this.value = value;
	}
	
	public static FieldType getType(byte typeId) {
		for (FieldType type : FieldType.values()) {
			if (type.value == typeId) return type;
		}
		
		return ERROR;
	}
	
	public static byte getFieldTypeId(FieldType type) {
		return (byte)type.value;
	}
	
	public static FieldType getTypeByName(String typeName) {
		for (FieldType type : FieldType.values()) {
			if (type.name().equals(typeName)) return type;
		}
		
		return ERROR;		
	}
}
