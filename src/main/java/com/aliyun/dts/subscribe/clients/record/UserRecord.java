package com.aliyun.dts.subscribe.clients.record;

public interface UserRecord {
    /**
     * Get the record unique id.
     */
    long getId();

    /**
     * Get the record source timestamp.
     */
    long getSourceTimestamp();

    /**
     * Get the operation which causes current record.
     */
    OperationType getOperationType();

    /**
     * Get the schema of current record data.
     */
    RecordSchema getSchema();

    /**
     * Get the before row image of current record.
     */
    RowImage getBeforeImage();

    /**
     * Get the after row image of current record.
     * @return
     */
    RowImage getAfterImage();
}
