package com.taobao.drc.togo.data;

/**
 * @author yangyang
 * @since 17/3/8
 */
public class SchemaNotFoundException extends Exception {
    private final int id;
    private final int version;

    public SchemaNotFoundException(int id, int version) {
        super("Schema [" + id + "][" + version + "] not found");
        this.id = id;
        this.version = version;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    public int getId() {
        return id;
    }

    public int getVersion() {
        return version;
    }
}
