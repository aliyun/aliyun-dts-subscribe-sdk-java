package com.aliyun.dts.subscribe.clients.record;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

public class ForeignKeyIndexInfo extends RecordIndexInfo {
    final String parentSchema;
    final String parentDatabase;
    final String parentTable;
    final TreeMap<String, RecordField> referencedColumn;
    public ForeignKeyIndexInfo(IndexType type, String parentSchema, String parentDatabase, String parentTable) {
        super(type);
        this.parentSchema = parentSchema;
        this.parentDatabase = parentDatabase;
        this.parentTable = parentTable;
        this.referencedColumn = new TreeMap<>();
    }

    public String getParentSchema() {
        return parentSchema;
    }

    public void addConstraintField(String parentColumn, RecordField currentField) {
        referencedColumn.put(parentColumn, currentField);
        super.addField(currentField);
    }

    public String getParentTable() {
        return parentTable;
    }

    public String getParentDatabase() {
        return parentDatabase;
    }

    public List<RecordField> getIndexFields() {
        List<RecordField> ret = new LinkedList<>();
        referencedColumn.forEach((k, v) -> ret.add(v));
        return ret;
    }
}
