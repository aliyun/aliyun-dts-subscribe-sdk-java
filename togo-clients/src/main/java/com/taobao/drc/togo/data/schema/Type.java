package com.taobao.drc.togo.data.schema;

import java.util.Locale;

/**
 * @author yangyang
 * @since 17/3/13
 */
public enum Type {
    BOOLEAN,
    INT32,
    INT64,
    FLOAT32,
    FLOAT64,
    STRING,
    BYTES,
    ARRAY,
    MAP,
    STRUCT;

    private String name;

    Type() {
        this.name = this.name().toLowerCase(Locale.ROOT);
    }

    public String getName() {
        return name;
    }

    public boolean isPrimitive() {
        switch (this) {
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BOOLEAN:
            case STRING:
            case BYTES:
                return true;
        }
        return false;
    }
}
