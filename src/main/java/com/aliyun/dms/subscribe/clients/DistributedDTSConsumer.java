package com.aliyun.dms.subscribe.clients;

import com.aliyun.dts.subscribe.clients.common.RecordListener;
import java.util.Map;

public interface DistributedDTSConsumer {


    void start();

    void addRecordListeners(Map<String, RecordListener> recordListeners);

}
