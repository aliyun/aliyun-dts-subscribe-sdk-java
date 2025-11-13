package com.aliyun.dts.subscribe.clients.recordprocessor;


import com.aliyun.dts.subscribe.clients.ConsumerContext;
import com.aliyun.dts.subscribe.clients.common.*;
import com.aliyun.dts.subscribe.clients.record.UserRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static com.aliyun.dts.subscribe.clients.common.Util.require;
import static com.aliyun.dts.subscribe.clients.common.Util.sleepMS;


/**
 * This demo show how to resolve avro record deserialize from bytes
 * We will show how to print a column from deserialize record
 */
public class EtlRecordProcessor implements  Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(EtlRecordProcessor.class);

    private final LinkedBlockingQueue<UserRecord> toProcessRecord;
    private final Map<String, RecordListener> recordListeners;

    private ConsumerContext consumerContext;

    public EtlRecordProcessor(ConsumerContext consumerContext, LinkedBlockingQueue<UserRecord> toProcessRecord,
                              Map<String, RecordListener> recordListeners) {
        this.consumerContext = consumerContext;
        this.toProcessRecord = toProcessRecord;
        this.recordListeners= recordListeners;
    }

    @Override
    public void run() {
        while (!consumerContext.isExited()) {
            UserRecord toProcess = null;
            int fetchFailedCount = 0;
            try {
                while (null == (toProcess = toProcessRecord.peek()) && !consumerContext.isExited()) {
                    sleepMS(5);
                    fetchFailedCount++;
                    if (fetchFailedCount % 1000 == 0 && consumerContext.hasValidTopicPartitions()) {
                        log.info("EtlRecordProcessor: haven't receive records from generator for  5s");
                    }
                }
                if (consumerContext.isExited()) {
                    return;
                }
                fetchFailedCount = 0;
                final UserRecord consumerRecord = toProcess;

                for (RecordListener recordListener : recordListeners.values()) {
                    recordListener.consume(consumerRecord);
                }

                toProcessRecord.poll();
            } catch (Exception e) {
                log.error("EtlRecordProcessor: process record failed, raw consumer record [" + toProcess + "],  cause " + e.getMessage(), e);
                consumerContext.exit();
            }
        }
    }

    public void registerRecordListener(String name, RecordListener recordListener) {
        require(null != name && null != recordListener, "null value not accepted");
        recordListeners.put(name, recordListener);
    }

    public  void close() {
        consumerContext.exit();
    }
}
