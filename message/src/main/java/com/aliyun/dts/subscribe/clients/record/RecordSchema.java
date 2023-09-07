package com.aliyun.dts.subscribe.clients.record;

import com.aliyun.dts.subscribe.clients.common.NullableOptional;

import java.util.List;

public interface RecordSchema {

    /**
     * get the database info the record schema refers to.
     * @return DatabaseInfo
     */
    DatabaseInfo getDatabaseInfo();

    /**
     * @return the list of fields that are present in the schema
     */
    List<RecordField> getFields();

    /**
     * @return the number of fields in the schema
     */
    int getFieldCount();

    /**
     * @param index the 0-based index of which field to return
     * @return the index'th field
     */
    RecordField getField(int index);

    /**
     * @param fieldName
     *            the name of the field
     * @return an Optional RecordField for the field with the given name
     */
    NullableOptional<RecordField> getField(String fieldName);


    /**
     * mark the @filed to be ignored, so the user can not see it any more
     * @param field to be ignored
     */
    void ignoreField(RecordField field);

    /**
     * @return the raw data types of the fields
     */
    List<Integer> getRawDataTypes();

    /**
     * @return the names of the fields
     */
    List<String> getFieldNames();

    /**
     * @param fieldName
     *            the name of the field whose type is desired
     * @return the RecordFieldType associated with the field that has the given name, or <code>null</code> if the schema does not contain a field with the given name
     */
    NullableOptional<Integer> getRawDataType(String fieldName);

    /**
     * @return the full name with qualified character of current record schema
     */
    NullableOptional<String> getFullQualifiedName();

    /**
     * @return the table name
     */
    NullableOptional<String> getDatabaseName();

    /**
     * get schema name
     * @return NullableOptional if schema name is empty
     */
    NullableOptional<String> getSchemaName();

    /**
     * getTableName
     * @return NullableOptional if table name is empty
     */
    NullableOptional<String> getTableName();

    /**
     * @return the id for this schema
     */
    String getSchemaIdentifier();

    /**
     * get the primary key info.
     * @return record index info
     */
    RecordIndexInfo getPrimaryIndexInfo();

    /**
     * get all foreign key info, may be the foreign key refers to a primary key with multi cols, so we
     * use RecordIndexInfo to represent it.
     * @return all foreign key info
     */
    List<ForeignKeyIndexInfo> getForeignIndexInfo();

    /**
     * get all unique key info.
     * @return all unique key info
     */
    List<RecordIndexInfo> getUniqueIndexInfo();

    /**
     * get all normal indexes(which means it's not pk, uk and fk).
     * @return all normal indexes
     */
    List<RecordIndexInfo> getNormalIndexInfo();

    /**
     * get the estimated total rows in current record schema.
     * @return total rows
     */
    default long getTotalRows() {
        return 0L;
    }

    String getFilterCondition();

    void initFilterCondition(String condition);

    default List<RecordField> getPartitionFields() {
        return null;
    }

    default void addUniqueIndexInfo(RecordIndexInfo indexInfo) {
        throw new RuntimeException("not impl");
    }

    default void addForeignIndexInfo(ForeignKeyIndexInfo indexInfo) {
        throw new RuntimeException("not impl");
    }

    /**
     * get the table charset
     * @return charset
     */
    default String getCharset() {
        return null;
    }
}
