package com.aliyun.dts.subscribe.clients.common;

import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import org.apache.kafka.common.TopicPartition;

public interface UserCommitCallBack {
    public void commit(TopicPartition tp, Record record, long offset, String metadata);
}
