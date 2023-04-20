package com.taobao.drc.togo.common.businesslogic;

/**
 * Created by longxuan on 18/1/17.
 */
public enum RecordType {
    OTHER((short)1),
    BEGIN((short)2),
    COMMIT((short)3),
    DDL((short)4),
    HEARTBEAT((short)5),
    DML((short)6),
    UNKNOWN((short)256);
    private short typeID;
    RecordType(short typeID) {
        this.typeID = typeID;
    }

    public short getTypeID() {
        return typeID;
    }

    private static RecordType[] idToMapEntry;
    private static int minSpecialID = OTHER.getTypeID();
    private static int maxSpecialID = HEARTBEAT.getTypeID();
    static {
        RecordType[] typeArray = new RecordType[UNKNOWN.getTypeID() + 1];
        for (RecordType recordType : RecordType.values()) {
            typeArray[recordType.getTypeID()] = recordType;
        }
        idToMapEntry = typeArray;
    }

    public static RecordType idToType(int id) {
        return idToMapEntry[id];
    }

    public static boolean isSpecialRecordType(RecordType recordType) {
        return recordType.getTypeID() >= 2 && recordType.getTypeID() <= 5;
    }

    public static boolean isSpecialType(int id) {
        return id >= minSpecialID && id <= maxSpecialID;
    }
}
