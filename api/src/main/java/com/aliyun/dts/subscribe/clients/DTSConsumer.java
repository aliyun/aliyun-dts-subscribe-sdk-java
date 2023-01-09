package com.aliyun.dts.subscribe.clients;

import java.util.Map;

public interface DTSConsumer {
    void start();

    void addRecordListeners(Map<String, RecordListener> recordListeners);

    boolean check();

    void close();
}

