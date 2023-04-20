package org.apache.kafka.common.record.meta;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public final class RecordSchema extends RecordMeta {
    private static final String NAME_KEY_NAME = "name";
    private static final String ID_KEY_NAME = "id";
    private static final String VERSION_KEY_NAME = "version";
    private static final String DATA_KEY_NAME = "data";

    private static final ByteBuffer fakeEmptyData = ByteBuffer.wrap(new byte[]{0});


    public String name;
    public int id;
    public short version;
    public ByteBuffer data;

    public RecordSchema() {
        this("");
    }

    public RecordSchema(String name) {
        this(name, fakeEmptyData);
    }

    public RecordSchema(String name, ByteBuffer data) {
        this(name, 0, 0, data);
    }

    public RecordSchema(String name, int id, int version, ByteBuffer data) {
        if (version > Short.MAX_VALUE) {
            throw new AssertionError(String.format("@version %d is too large to represent as short", version));
        }

        this.name = name;
        this.id = id;
        this.version = (short) version;
        this.data = data;
    }

    public RecordSchema(String name, int id, int version, Errors error) {
        this(name, id, version, fakeEmptyData);
        setError(error);
    }

    public static RecordSchema parseFrom(Struct struct) {
        return new RecordSchema(
                struct.getString(NAME_KEY_NAME),
                struct.getInt(ID_KEY_NAME),
                struct.getShort(VERSION_KEY_NAME),
                struct.getBytes(DATA_KEY_NAME));
    }

    @Override
    public Struct toStruct(String keyName, Struct parent) {
        Struct ret = parent.instance(keyName);
        ret.set(NAME_KEY_NAME, name);
        ret.set(ID_KEY_NAME, id);
        ret.set(VERSION_KEY_NAME, version);
        ret.set(DATA_KEY_NAME, data);
        return ret;
    }

    @Override
    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public short getVersion() {
        return version;
    }

    public ByteBuffer getData() {
        return data;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("(name=").append(name).
                append(", id=").append(id).
                append(", version=").append(version).
                append(", data=").append(data).
                append(")");
        return bld.toString();
    }
}
