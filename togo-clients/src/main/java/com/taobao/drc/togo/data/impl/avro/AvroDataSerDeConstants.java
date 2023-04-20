package com.taobao.drc.togo.data.impl.avro;

import com.taobao.drc.togo.data.schema.Schema;

import java.nio.ByteOrder;

/**
 * @author yangyang
 * @since 17/3/28
 */
public class AvroDataSerDeConstants {
    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    public static final byte MAGIC = 0;
    public static final int MAGIC_OFFSET = 0;
    public static final int MAGIC_SIZE = 1;
    public static final int ATTRIBUTE_OFFSET = MAGIC_OFFSET + MAGIC_SIZE;
    public static final int ATTRIBUTE_SIZE = 1;
    public static final int SCHEMA_ID_OFFSET = ATTRIBUTE_OFFSET + ATTRIBUTE_SIZE;
    public static final int SCHEMA_ID_SIZE = 4;
    public static final int SCHEMA_VERSION_OFFSET = SCHEMA_ID_OFFSET + SCHEMA_ID_SIZE;
    public static final int SCHEMA_VERSION_SIZE = 2;
    public static final int HEADER_SIZE = SCHEMA_VERSION_OFFSET + SCHEMA_VERSION_SIZE;
    public static final int FIELD_INDEX_OFFSET = HEADER_SIZE;
    public static final int SIZE_OF_FIELD_INDEX = 4;

    public final static int ATTRIBUTE_WITH_FIELD_OFFSET = 1;

    public static int serializedRecordSize(Schema schema, int recordSize) {
        return serializedRecordSize(schema, recordSize, true);
    }

    public static int serializedRecordSize(Schema schema, int recordSize, boolean withIndex) {
        if (withIndex) {
            int indexSize = SIZE_OF_FIELD_INDEX + SIZE_OF_FIELD_INDEX * schema.fields().size();
            return indexSize + serializedRecordSizeWithoutIndex(schema, recordSize);
        } else {
            return serializedRecordSizeWithoutIndex(schema, recordSize);
        }
    }

    private static int serializedRecordSizeWithoutIndex(Schema schema, int recordSize) {
        return HEADER_SIZE + recordSize;
    }

    public static int fieldIndexSize(int fieldNum) {
        return (1 + fieldNum) * SIZE_OF_FIELD_INDEX;
    }

    public static int fieldOffset(int index) {
        return (1 + index) * SIZE_OF_FIELD_INDEX;
    }
}
