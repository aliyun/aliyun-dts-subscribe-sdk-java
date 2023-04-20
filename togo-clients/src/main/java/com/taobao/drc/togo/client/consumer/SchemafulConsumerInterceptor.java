package com.taobao.drc.togo.client.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @author yangyang
 * @since 17/4/19
 */
public interface SchemafulConsumerInterceptor extends Configurable {
    /**
     * This is called just before the records are returned by {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(long)}
     * <p>
     * This method is allowed to modify consumer records, in which case the new records will be
     * returned. There is no limitation on number of records that could be returned from this
     * method. I.e., the interceptor can filter the records or generate new records.
     * <p>
     * Any exception thrown by this method will be caught by the caller, logged, but not propagated to the client.
     * <p>
     * Since the consumer may run multiple interceptors, a particular interceptor's onConsume() callback will be called
     * in the order specified by {@link org.apache.kafka.clients.consumer.ConsumerConfig#INTERCEPTOR_CLASSES_CONFIG}.
     * The first interceptor in the list gets the consumed records, the following interceptor will be passed the records returned
     * by the previous interceptor, and so on. Since interceptors are allowed to modify records, interceptors may potentially get
     * the records already modified by other interceptors. However, building a pipeline of mutable interceptors that depend on the output
     * of the previous interceptor is discouraged, because of potential side-effects caused by interceptors potentially failing
     * to modify the record and throwing an exception. If one of the interceptors in the list throws an exception from onConsume(),
     * the exception is caught, logged, and the next interceptor is called with the records returned by the last successful interceptor
     * in the list, or otherwise the original consumed records.
     *
     * @param records records to be consumed by the client or records returned by the previous interceptors in the list.
     * @return records that are either modified by the interceptor or same as records passed to this method.
     */
    SchemafulConsumerRecords onConsume(SchemafulConsumerRecords records);

    /**
     * This is called when offsets get committed.
     * <p>
     * Any exception thrown by this method will be ignored by the caller.
     *
     * @param offsets A map of offsets by partition with associated metadata
     */
    void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * This is called when interceptor is closed
     */
    void close();
}
