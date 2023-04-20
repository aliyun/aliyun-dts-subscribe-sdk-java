package com.taobao.drc.togo.client.producer;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Created by longxuan on 2018/6/4.
 */
public class LeaderSwitchInterruptListener implements Metadata.Listener {
    private final static Logger logger = LoggerFactory.getLogger(LeaderSwitchInterruptListener.class);
    private Cluster originCluster = null;
    private final TogoProducer.TogoAdaptedKafkaProducer togoAdaptedKafkaProducer;

    public LeaderSwitchInterruptListener(TogoProducer.TogoAdaptedKafkaProducer togoAdaptedKafkaProducer) {
        this.togoAdaptedKafkaProducer = togoAdaptedKafkaProducer;
    }

    @Override
    public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
        if (null == originCluster) {
            logger.info("LeaderSwitchInterruptListener: first update, receive cluster info [{}]", cluster);
            originCluster = cluster;
        } else {
            Cluster originClusterSaved = originCluster;
            originCluster = cluster;
            checkClusterSwitch(originClusterSaved, cluster);
        }
    }

    private void checkClusterSwitch(Cluster originCluster, Cluster refreshedCluster) {
        if (null == originCluster || null == refreshedCluster) {
            logger.warn("LeaderSwitchInterruptListener: invalid comparator originCLuster [{}], refreshedCluster [{}]", originCluster, refreshedCluster);
            return;
        }
        originCluster.topics().forEach(topic->{
            List<PartitionInfo> originPartitionInfoList = originCluster.partitionsForTopic(topic);
            List<PartitionInfo> refreshedPartitionInfoList = refreshedCluster.partitionsForTopic(topic);
            checkPartitions(topic, originPartitionInfoList, refreshedPartitionInfoList);
        });
    }

    private void checkPartitions(String topic, List<PartitionInfo> originPartitionInfoList, List<PartitionInfo> refreshedPartitionInfoList) {
        originPartitionInfoList.forEach(originPartitionInfo -> {
            PartitionInfo refreshedPartitionInfo = refreshedPartitionInfoList.stream().filter(partitionInfo -> {
                return partitionInfo.partition() == originPartitionInfo.partition();
            }).findFirst().orElseGet(() -> null);
            if (null == refreshedPartitionInfo) {
                logger.warn("LeaderSwitchInterruptListener: topic [{}] removed, ignore", topic);
            }
            if (!refreshedPartitionInfo.leader().equals(originPartitionInfo.leader())) {
                String message = "LeaderSwitchInterruptListener: leader changed for topic [" + topic + " ], " +
                        "from [" + originPartitionInfo + "] to [ " + refreshedPartitionInfo + " ]";
                togoAdaptedKafkaProducer.notifyLeaderSwitch(message);
                logger.warn(message);
                throw new LeaderSwitchException(message);
            }
        });
    }

    public static class LeaderSwitchException extends RuntimeException {
        public LeaderSwitchException(String message, Throwable cause) {
            super(message, cause);
        }

        public LeaderSwitchException(String message) {
            super(message);
        }

        public LeaderSwitchException(Throwable cause) {
            super(cause);
        }

        public LeaderSwitchException() {
            super();
        }
    }
}
