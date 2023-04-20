package com.taobao.drc.togo.util;

import org.apache.kafka.common.record.Record;

/**
 * Created by longxuan on 2018/11/1.
 */
public interface ConsumerSpeedStaticFunc {
    public void metric(long offset, long count, long bytes, Record record);
}
