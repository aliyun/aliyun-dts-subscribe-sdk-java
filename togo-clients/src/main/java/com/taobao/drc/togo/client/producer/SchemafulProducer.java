package com.taobao.drc.togo.client.producer;

import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.SchemaMetaData;
import com.taobao.drc.togo.util.concurrent.TogoCallback;
import com.taobao.drc.togo.util.concurrent.TogoFuture;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @see TogoProducer
 */
public interface SchemafulProducer extends Closeable {

    TogoFuture<SchemaMetaData> registerSchema(String topic, String name, Schema schema);

    TogoFuture<RecordMetadata> send(SchemafulProducerRecord record);

    TogoFuture<RecordMetadata> send(SchemafulProducerRecord record, TogoCallback<RecordMetadata> callback);

    void flush();

    List<PartitionInfo> partitionsFor(String topic);

    Map<MetricName, ? extends Metric> metrics();

    void close();

    void close(long timeout, TimeUnit unit);
}
