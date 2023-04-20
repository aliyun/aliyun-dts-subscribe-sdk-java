package com.taobao.drc.togo.util;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by longxuan on 17/12/20.
 */
public class SetOffsetMeta {
    private final Map<TopicPartition, Long> offsetPartitionMeta;
    private final Long defaultOffset;

    public SetOffsetMeta(Long defaultOffset) {
        this.offsetPartitionMeta = new HashMap<>();
        this.defaultOffset = defaultOffset;
    }

    public boolean setOffsetForTP(TopicPartition tp, Long offset) {
        if (offsetPartitionMeta.containsKey(tp)) {
            return false;
        } else {
            offsetPartitionMeta.put(tp, offset);
            return true;
        }
    }

    public Long retrieveOffset(TopicPartition topicPartition) {
        Long ret = offsetPartitionMeta.get(topicPartition);
        if (null == ret) {
            ret = defaultOffset;
            offsetPartitionMeta.put(topicPartition, defaultOffset);
        } else {
            if (defaultOffset != ret) {
                offsetPartitionMeta.put(topicPartition, defaultOffset);
            }
        }
        return ret;

    }
}
