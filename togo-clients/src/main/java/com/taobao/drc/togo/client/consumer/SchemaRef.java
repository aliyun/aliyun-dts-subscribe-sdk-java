package com.taobao.drc.togo.client.consumer;

import java.util.Objects;

/**
 * @author yangyang
 * @since 17/3/31
 */
class SchemaRef {
    private final String topic;
    private final int schemaId;
    private final int version;

    SchemaRef(String topic, int schemaId, int version) {
        this.topic = topic;
        this.schemaId = schemaId;
        this.version = version;
    }

    public String getTopic() {
        return topic;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaRef schemaRef = (SchemaRef) o;
        return schemaId == schemaRef.schemaId &&
                version == schemaRef.version &&
                Objects.equals(topic, schemaRef.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, schemaId, version);
    }

    @Override
    public String toString() {
        return "SchemaRef{" +
                "topic='" + topic + '\'' +
                ", schemaId=" + schemaId +
                ", version=" + version +
                '}';
    }
}
