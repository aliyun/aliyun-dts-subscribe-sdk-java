package com.aliyun.dts.subscribe.clients.record;

public interface UserRecord {
    /**
     * @return Get the record unique id.
     */
    long getId();

    /**
     * @return Get the record source timestamp.
     */
    long getSourceTimestamp();

    /**
     * @return Get the operation which causes current record.
     */
    OperationType getOperationType();

    /**
     * @return Get the schema of current record data.
     */
    RecordSchema getSchema();

    /**
     * @return Get the before row image of current record.
     */
    RowImage getBeforeImage();

    /**
     * @return Get the after row image of current record.
     */
    RowImage getAfterImage();
}
