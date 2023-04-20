package com.taobao.drc.togo.client.producer;

import com.taobao.drc.togo.data.SchemafulRecord;
import com.taobao.drc.togo.data.schema.SchemaMetaData;

import java.util.Objects;

/**
 * @author yangyang
 * @since 17/3/8
 */
public class SchemafulProducerRecord {
    private final String topic;
    private final Integer partition;
    private final Long timestamp;
    private final SchemafulRecord value;
    private final SchemaMetaData schemaMetaData;
    private final byte[] data;

    public SchemafulProducerRecord(String topic, Integer partition, Long timestamp,
                                   SchemafulRecord value, SchemaMetaData schemaMetaData, byte[] data) {

        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null.");
        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
        if (partition != null && partition < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition number should always be non-negative or null.", partition));
        this.topic = topic;
        this.partition = partition;
        this.timestamp = timestamp;
        this.value = Objects.requireNonNull(value);
        this.data = Objects.requireNonNull(data);
        this.schemaMetaData = Objects.requireNonNull(schemaMetaData);
    }

    public SchemafulProducerRecord(String topic, Integer partition,
                                   SchemafulRecord value, SchemaMetaData schemaMetaData, byte[] data) {
        this(topic, partition, null, value, schemaMetaData, data);
    }

    public SchemafulProducerRecord(String topic, SchemafulRecord value, SchemaMetaData schemaMetaData, byte[] data) {
        this(topic, null, value, schemaMetaData, data);
    }

    public String topic() {
        return topic;
    }

    public Integer partition() {
        return partition;
    }

    public Long timestamp() {
        return timestamp;
    }

    public SchemafulRecord value() {
        return value;
    }

    public SchemaMetaData schemaMetaData() {
        return schemaMetaData;
    }

    public byte[] data() {
        return data;
    }
}
