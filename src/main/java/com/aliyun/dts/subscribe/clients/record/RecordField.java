package com.aliyun.dts.subscribe.clients.record;

import com.aliyun.dts.subscribe.clients.record.value.Value;

public interface RecordField {
    /**
     * @return Get the field name, which is case sensitive.
     */
    String getFieldName();

    /**
     * @return Get raw data type of this field.
     */
    int getRawDataTypeNum();

    /**
     * @return Get default value of current field.
     */
    Value getDefaultValue();

    /**
     * @return Determine if current field is nullable.
     */
    boolean isNullable();

    /**
     * @return Determine if current field is an element of uk.
     */
    boolean isUnique();

    /**
     * @return set if current field is an element of uk.
     */
    RecordField setUnique(boolean unique);

    /**
     * @return Determine if current field is an element of pk.
     */
    boolean isPrimary();

    /**
     * @return Determine if current field is an element of some index.
     */
    boolean isIndexed();

    /**
     * @return Determine if current field is auto incremental.
     */
    boolean isAutoIncrement();

    /**
     * @return Get field position to set/get value, which starts from 0.
     */
    int getFieldPosition();

    /**
     * @return Set field position to set/get value, which starts from 0.
     */
    void setFieldPosition(int position);

}
