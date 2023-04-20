package com.taobao.drc.togo.client.consumer;

import org.apache.kafka.common.requests.FlexListOffsetRequest;

/**
 * @author yangyang
 * @since 17/3/14
 */
public interface QueryCondition {
    FlexListOffsetRequest.IndexEntry createRequestEntry();
}
