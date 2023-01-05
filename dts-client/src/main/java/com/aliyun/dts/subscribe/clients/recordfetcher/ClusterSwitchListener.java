package com.aliyun.dts.subscribe.clients.recordfetcher;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *  We recommend user register this listener.
 *  Cause when origin cluster is unavailable and new cluster is created by HA(high available service).
 *  The cluster name is different. We want warn user that a new cluster is working.
 *  The more important thing is that we want user recreate KakfaConsumer and use timestamp to reseek offset.
 *  If user following this guid, less duplicated data will be pushed.
 *  Otherwise
 */
public class ClusterSwitchListener implements ClusterResourceListener, ConsumerInterceptor {
    private final static Logger LOG = LoggerFactory.getLogger(ClusterSwitchListener.class);
    private ClusterResource originClusterResource = null;
    private ClusterResource currentClusterResource = null;

    public ConsumerRecords onConsume(ConsumerRecords records) {
        return records;
    }


    public void close() {
    }

    public void onCommit(Map offsets) {
    }


    public void onUpdate(ClusterResource clusterResource) {
        synchronized (this) {
            originClusterResource = currentClusterResource;
            currentClusterResource = clusterResource;
            if (null == originClusterResource) {
                LOG.info("Cluster updated to " + currentClusterResource.clusterId());
            } else {
                if (originClusterResource.clusterId().equals(currentClusterResource.clusterId())) {
                    LOG.info("Cluster not changed on update:" + clusterResource.clusterId());
                } else {
                    LOG.error("Cluster changed");
                    throw new ClusterSwitchException("Cluster changed from " + originClusterResource.clusterId() + " to " + currentClusterResource.clusterId()
                            + ", consumer require restart");
                }
            }
        }
    }

    public boolean isClusterResourceChanged() {
        if (null == originClusterResource) {
            return false;
        }
        if (originClusterResource.clusterId().equals(currentClusterResource.clusterId())) {
            return false;
        }
        return true;
    }

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

