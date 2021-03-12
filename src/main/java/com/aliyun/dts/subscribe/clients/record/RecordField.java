package com.aliyun.dts.subscribe.clients.record;

import com.aliyun.dts.subscribe.clients.record.value.Value;

public interface RecordField {
    /**
     * Get the field name, which is case sensitive.
     */
    String getFieldName();

    /**
     * Get raw data type of this field.
     */
    int getRawDataTypeNum();

    /**
     * Get default value of current field.
     */
    Value getDefaultValue();

    /**
     * Determine if current field is nullable.
     */
    boolean isNullable();

    /**
     * Determine if current field is an element of uk.
     */
    boolean isUnique();

    /**
     * set if current field is an element of uk.
     */
    RecordField setUnique(boolean unique);

    /**
     * Determine if current field is an element of pk.
     */
    boolean isPrimary();

    /**
     * Determine if current field is an element of some index.
     */
    boolean isIndexed();

    /**
     * Determine if current field is auto incremental.
     */
    boolean isAutoIncrement();

    /**
     * Get field position to set/get value, which starts from 0.
     */
    int getFieldPosition();

    /**
     * Set field position to set/get value, which starts from 0.
     */
    void setFieldPosition(int position);

}
