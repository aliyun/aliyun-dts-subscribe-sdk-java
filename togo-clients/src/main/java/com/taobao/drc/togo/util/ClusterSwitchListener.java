package com.taobao.drc.togo.util;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by longxuan on 18/3/28.
 */
public class ClusterSwitchListener implements ClusterResourceListener, ConsumerInterceptor {
    private final static Logger logger = LoggerFactory.getLogger(ClusterSwitchListener.class);
    private ClusterResource originClusterResource = null;
    @Override
    public ConsumerRecords onConsume(ConsumerRecords records) {
        return records;
    }

    @Override
    public void close() {
    }

    @Override
    public void onCommit(Map offsets) {
    }


    @Override
    public void onUpdate(ClusterResource clusterResource) {
        synchronized (this) {
            if (null == originClusterResource) {
                logger.info("Cluster updated to " + clusterResource.clusterId());
                originClusterResource = clusterResource;
            } else {
                if (clusterResource.clusterId().equals(originClusterResource.clusterId())) {
                    logger.info("Cluster not changed on update:" + clusterResource.clusterId());
                } else {
                    throw new ClusterSwitchException("Cluster changed from " + originClusterResource.clusterId() + " to " + clusterResource.clusterId()
                        + ", consumer require restart");
                }
            }
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    public static class ClusterSwitchException extends KafkaException {
        public ClusterSwitchException(String message, Throwable cause) {
            super(message, cause);
        }

        public ClusterSwitchException(String message) {
            super(message);
        }

        public ClusterSwitchException(Throwable cause) {
            super(cause);
        }

        public ClusterSwitchException() {
            super();
        }

    }
}
