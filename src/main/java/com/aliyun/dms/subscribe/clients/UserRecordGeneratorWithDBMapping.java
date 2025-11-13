package com.aliyun.dms.subscribe.clients;

import com.aliyun.dts.subscribe.clients.ConsumerContext;
import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;
import com.aliyun.dts.subscribe.clients.record.UserRecord;
import com.aliyun.dts.subscribe.clients.recordfetcher.OffsetCommitCallBack;
import com.aliyun.dts.subscribe.clients.recordgenerator.UserRecordGenerator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.aliyun.dts.subscribe.clients.common.Util.sleepMS;

/**
 * This class is to resolve avro record deserialize from bytes to UserRecord
 */
public class UserRecordGeneratorWithDBMapping extends UserRecordGenerator {
    private static final Logger log = LoggerFactory.getLogger(UserRecordGeneratorWithDBMapping.class);

    public UserRecordGeneratorWithDBMapping(ConsumerContext consumerContext, LinkedBlockingQueue<ConsumerRecord> toProcessRecord,
                                            LinkedBlockingQueue<UserRecord> processedRecord,
                                            OffsetCommitCallBack offsetCommitCallBack) {
        super(consumerContext, toProcessRecord, processedRecord, offsetCommitCallBack);
    }

    @Override
    public void run() {
        while (!consumerContext.isExited()) {
            ConsumerRecord<byte[], byte[]> toProcess = null;
            Record record = null;
            int fetchFailedCount = 0;
            try {
                while (null == (toProcess = toProcessRecord.peek()) && !consumerContext.isExited()) {
                    sleepMS(5);
                    fetchFailedCount++;
                    if (fetchFailedCount % 1000 == 0 && consumerContext.hasValidTopicPartitions()) {
                        log.info("UserRecordGenerator: haven't receive records from generator for  5s");
                    }
                }
                if (consumerContext.isExited()) {
                    return;
                }
                final ConsumerRecord<byte[], byte[]> consumerRecord = toProcess;
                consumerRecord.timestamp();
                record = fastDeserializer.deserialize(consumerRecord.value());
                log.debug("UserRecordGenerator: meet [{}] record type", record.getOperation());

                if (consumerContext.getDbMapper() != null && consumerContext.getDbMapper().isMapping()) {
                    record = consumerContext.getDbMapper().transform(record);
                }
                DefaultUserRecord defaultUserRecord = new DefaultUserRecord(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset(),
                        record,
                        (tp, sourceTimestamp, offset, metadata) -> {
                            recordStoreOutCountSensor.record(1);
                            recordStoreOutByteSensor.record(consumerRecord.value().length);
                            commitCheckpoint = new Checkpoint(tp, sourceTimestamp, offset, metadata);
                            commit();
                        });

                int offerTryCount = 0;

                while (!offerRecord(1000, TimeUnit.MILLISECONDS, defaultUserRecord) && !consumerContext.isExited()) {
                    if (++offerTryCount % 10 == 0) {
                        log.info("UserRecordGenerator: offer user record has failed for a period (10s) [ " + record + "]");
                    }
                }

                toProcessRecord.poll();
            } catch (Exception e) {
                log.error("UserRecordGenerator: process record failed, raw consumer record [" + toProcess + "], parsed record [" + record + "], cause " + e.getMessage(), e);
                consumerContext.exit();
            }
        }
    }

    // user define how to commit
    private void commit() {
        if (null != offsetCommitCallBack) {
            if (commitCheckpoint.getTopicPartition() != null && commitCheckpoint.getOffset() != -1) {
                offsetCommitCallBack.commit(commitCheckpoint.getTopicPartition(), commitCheckpoint.getTimeStamp(),
                        commitCheckpoint.getOffset(), commitCheckpoint.getInfo());
            }
        }
    }
}
