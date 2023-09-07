package com.aliyun.dts.subscribe.clients.record;

import com.aliyun.dts.subscribe.clients.record.value.Value;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.function.Function;

public interface RowImage {

    /**
     * <p>
     * @return a view of the the values of the fields in this Record. Note that this method returns values only for
     * those entries in the Record's schema. This allows the Record to guarantee that it will return the values in
     * the order dictated by the schema.
     * </p>
     *
     * <b>NOTE:</b> The array that is returned may be an underlying array that is backing
     * the contents of the Record. As such, modifying the array in any way may result in
     * modifying the record.
     */
    Value[] getValues();

    /**
     * @param pos the position of the value
     * @return the value of specified @pos.
     */
    Value getValue(int pos);

    /**
     * @param fieldName the field name
     * @return the value of specified @fieldName.
     * This method is different as other getValue, for the field matched @fileName should never exist.
     */
    Value getValue(String fieldName);

    /**
     * @param recordField record filed
     * @return the value of specified @recordField.
     */
    Value getValue(RecordField recordField);

    /**
     * @return the primary keys of current row image.
     */
    Pair<RecordField, Value>[] getPrimaryKeyValues();

    /**
     * @return the merged field and value pairs fo all unique keys in current record.
     */
    Pair<RecordField, Value>[] getUniqueKeyValues();

    /**
     * @return the foreign keys of current row image.
     */
    Pair<RecordField, Value>[] getForeignKeyValues();

    /**
     * Converts the Record into a Map whose keys are the same as the Record's field names and the values are the field values
     * @param filedNameResolver field name resolver
     * @param valueResolver value resolver
     * @return a Map that represents the values in the Record
     */
    Map<String, Value> toMap(Function<String, String> filedNameResolver, Function<Value, Value> valueResolver);

    /**
     * The total size of all values in current row image.
     * @return size
     */
    long size();
}
