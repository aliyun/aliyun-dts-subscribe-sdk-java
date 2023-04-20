package com.taobao.drc.togo.client.consumer;

import com.taobao.drc.togo.client.producer.SchemafulProducerInterceptors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author yangyang
 * @since 17/4/19
 */
public class SchemafulConsumerInterceptors implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(SchemafulProducerInterceptors.class);
    private final List<SchemafulConsumerInterceptor> interceptors;

    public SchemafulConsumerInterceptors(List<SchemafulConsumerInterceptor> interceptors) {
        this.interceptors = interceptors;
    }

    public SchemafulConsumerRecords onConsume(SchemafulConsumerRecords records) {
        SchemafulConsumerRecords interceptRecords = records;
        for (SchemafulConsumerInterceptor interceptor : interceptors) {
            try {
                interceptRecords = interceptor.onConsume(interceptRecords);
            } catch (Exception e) {
                // do not propagate interceptor exception, log and continue calling other interceptors
                logger.warn("Error executing interceptor onConsume callback", e);
            }
        }
        return interceptRecords;
    }

    void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (SchemafulConsumerInterceptor interceptor : interceptors) {
            try {
                interceptor.onCommit(offsets);
            } catch (Exception e) {
                // do not propagate interceptor exception, just log
                logger.warn("Error executing interceptor onCommit callback", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (SchemafulConsumerInterceptor interceptor : interceptors) {
            try {
                interceptor.close();
            } catch (Exception e) {
                logger.error("Failed to close consumer interceptor ", e);
            }
        }
    }
}
