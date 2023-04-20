package com.taobao.drc.togo.client.producer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * @author yangyang
 * @since 17/4/19
 */
public class SchemafulProducerInterceptors implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(SchemafulProducerInterceptors.class);
    private final List<SchemafulProducerInterceptor> interceptors;

    public SchemafulProducerInterceptors(List<SchemafulProducerInterceptor> interceptors) {
        this.interceptors = interceptors;
    }

    public SchemafulProducerRecord onSend(SchemafulProducerRecord record) {
        SchemafulProducerRecord interceptRecord = record;
        for (SchemafulProducerInterceptor interceptor : interceptors) {
            try {
                interceptRecord = interceptor.onSend(interceptRecord);
            } catch (Exception e) {
                // do not propagate interceptor exception, log and continue calling other interceptors
                // be careful not to throw exception from here
                if (record != null)
                    logger.warn("Error executing interceptor onSend callback for topic: {}, partition: {}", record.topic(), record.partition(), e);
                else
                    logger.warn("Error executing interceptor onSend callback", e);
            }
        }
        return interceptRecord;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        for (SchemafulProducerInterceptor interceptor : this.interceptors) {
            try {
                interceptor.onAcknowledgement(metadata, exception);
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                logger.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    public void onSendError(SchemafulProducerRecord record, TopicPartition interceptTopicPartition, Exception exception) {
        for (SchemafulProducerInterceptor interceptor : this.interceptors) {
            try {
                if (record == null && interceptTopicPartition == null) {
                    interceptor.onAcknowledgement(null, exception);
                } else {
                    if (interceptTopicPartition == null) {
                        interceptTopicPartition = new TopicPartition(record.topic(),
                                record.partition() == null ? RecordMetadata.UNKNOWN_PARTITION : record.partition());
                    }
                    interceptor.onAcknowledgement(new RecordMetadata(interceptTopicPartition, -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1, -1),
                            exception);
                }
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                logger.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (SchemafulProducerInterceptor interceptor : interceptors) {
            try {
                interceptor.close();
            } catch (Exception e) {
                logger.error("Failed to close producer interceptor ", e);
            }
        }
    }
}
