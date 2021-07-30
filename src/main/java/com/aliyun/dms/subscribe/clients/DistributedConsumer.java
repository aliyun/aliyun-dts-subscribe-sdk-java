package com.aliyun.dms.subscribe.clients;

import com.aliyun.dts.subscribe.clients.DTSConsumer;
import com.aliyun.dts.subscribe.clients.common.RecordListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface DistributedConsumer {


    void start();

    void addRecordListeners(Map<String, RecordListener> recordListeners);

}
