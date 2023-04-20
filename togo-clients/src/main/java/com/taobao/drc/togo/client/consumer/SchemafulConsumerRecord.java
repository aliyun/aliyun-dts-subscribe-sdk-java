package com.taobao.drc.togo.client.consumer;

import com.taobao.drc.togo.data.SchemaAndValue;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

/**
 * @author yangyang
 * @since 17/3/28
 */
public class SchemafulConsumerRecord {
    public static final long NO_TIMESTAMP = RecordBatch.NO_TIMESTAMP;
    public static final int NULL_SIZE = -1;
    public static final int NULL_CHECKSUM = -1;

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final TimestampType timestampType;
    private final long checksum;
    private final int serializedValueSize;
    private final SchemaAndValue schemaAndValue;
    private final byte[] data;

    public SchemafulConsumerRecord(String topic,
                                   int partition,
                                   long offset,
                                   long timestamp,
                                   TimestampType timestampType,
                                   long checksum,
                                   int serializedValueSize,
                                   SchemaAndValue schemaAndValue, byte[] data) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.checksum = checksum;
        this.serializedValueSize = serializedValueSize;
        this.schemaAndValue = schemaAndValue;
        this.data = data;
    }

    /**
     * The topic this record is received from
     */
    public String topic() {
        return this.topic;
    }

    /**
     * The partition from which this record is received
     */
    public int partition() {
        return this.partition;
    }

    public SchemaAndValue schemaAndValue() {
        return schemaAndValue;
    }

    /**
     * The position of this record in the corresponding Kafka partition.
     */
    public long offset() {
        return offset;
    }

    /**
     * The timestamp of this record
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * The timestamp type of this record
     */
    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * The checksum (CRC32) of the record.
     */
    public long checksum() {
        return this.checksum;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the
     * returned size is -1.
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    public byte[] data() {
        return this.data;
    }
}
