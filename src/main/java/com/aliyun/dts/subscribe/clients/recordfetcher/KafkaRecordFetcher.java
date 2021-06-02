package com.aliyun.dts.subscribe.clients.recordfetcher;

import com.aliyun.dts.subscribe.clients.ConsumerContext;
import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import com.aliyun.dts.subscribe.clients.exception.TimestampSeekException;
import com.aliyun.dts.subscribe.clients.metastore.KafkaMetaStore;
import com.aliyun.dts.subscribe.clients.metastore.LocalFileMetaStore;
import com.aliyun.dts.subscribe.clients.metastore.MetaStoreCenter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.SimpleRate;
import org.apache.kafka.common.metrics.stats.Total;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aliyun.dts.subscribe.clients.common.Util.sleepMS;
import static com.aliyun.dts.subscribe.clients.common.Util.swallowErrorClose;

public class KafkaRecordFetcher implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(KafkaRecordFetcher.class);
    private static final String LOCAL_FILE_STORE_NAME = "localCheckpointStore";
    private static final String KAFKA_STORE_NAME = "kafkaCheckpointStore";
    private static final String USER_STORE_NAME = "userCheckpointStore";

    private ConsumerContext consumerContext;
    private LinkedBlockingQueue<ConsumerRecord> toProcessRecord;

    //private volatile boolean existed;
    private final int tryTime;
    private final long tryBackTimeMS;

    private final AtomicBoolean useCheckpointConfig;
    private final Checkpoint initialCheckpoint;
    private volatile Checkpoint toCommitCheckpoint = null;
    private final ConsumerContext.ConsumerSubscribeMode subscribeMode;

    private final TopicPartition topicPartition;
    private final String groupID;

    private final MetaStoreCenter metaStoreCenter = new MetaStoreCenter();

    private long nextCommitTimestamp;

    private final Sensor recordStoreInCountSensor;
    private final Sensor recordStoreInByteSensor;

    public KafkaRecordFetcher(ConsumerContext consumerContext, LinkedBlockingQueue<ConsumerRecord> toProcessRecord) {
        this.consumerContext = consumerContext;
        this.toProcessRecord = toProcessRecord;

        this.useCheckpointConfig = new AtomicBoolean(consumerContext.isForceUseCheckpoint());
        this.initialCheckpoint = consumerContext.getInitialCheckpoint();
        this.subscribeMode = consumerContext.getSubscribeMode();

        this.topicPartition = new TopicPartition(consumerContext.getTopic(), 0);
        this.groupID = consumerContext.getGroupID();

        this.tryTime = 150;
        this.tryBackTimeMS = 10000;

        //existed = false;
        metaStoreCenter.registerStore(composeLocalFileStoreName(LOCAL_FILE_STORE_NAME, groupID), new LocalFileMetaStore(composeLocalFileStoreName(LOCAL_FILE_STORE_NAME, groupID)));

        if (consumerContext.getUserRegisteredStore() != null) {
            metaStoreCenter.registerStore(USER_STORE_NAME, consumerContext.getUserRegisteredStore());
        }

        log.info("RecordGenerator: try time [" + tryTime + "], try backTimeMS [" + tryBackTimeMS + "]");

        Metrics metrics = consumerContext.getDtsMetrics().getCoreMetrics();
        this.recordStoreInCountSensor = metrics.sensor("record-store-in-row");
        this.recordStoreInCountSensor.add(metrics.metricName("inCounts", "recordstore"), new Total());
        this.recordStoreInCountSensor.add(metrics.metricName("inRps", "recordstore"), new SimpleRate());
        this.recordStoreInByteSensor = metrics.sensor("record-store-in-byte");
        this.recordStoreInByteSensor.add(metrics.metricName("inBytes", "recordstore"), new Total());
        this.recordStoreInByteSensor.add(metrics.metricName("inBps", "recordstore"), new SimpleRate());
    }

    @Override
    public void run() {

        int haveTryTime = 0;
        String message = "first start";
        ConsumerWrap kafkaConsumerWrap = null;
        while (!consumerContext.isExited()) {
            try {
                kafkaConsumerWrap = getConsumerWrap(message);
                while (!consumerContext.isExited()) {
                    // kafka consumer is not threadsafe, so if you want commit checkpoint to kafka, commit it in same thread
                    mayCommitCheckpoint();
                    ConsumerRecords<byte[], byte[]> records = kafkaConsumerWrap.poll();
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        int offerTryCount = 0;
                        if (record.value() == null || record.value().length <= 2) {
                            // dStore may generate special mock record to push up consumer offset for next fetchRequest if all data is filtered
                            continue;
                        }
                        while (!offerRecord(1000, TimeUnit.MILLISECONDS, record) && !consumerContext.isExited()) {
                            if (++offerTryCount % 10 == 0) {
                                log.info("KafkaRecordFetcher: offer kafka record has failed for a period (10s) [ " + record + "]");
                            }
                        }
                    }
                }
            } catch (Throwable e) {
                if (isErrorRecoverable(e) && haveTryTime++ < tryTime) {
                    log.warn("KafkaRecordFetcher: error meet cause " + e.getMessage() + ", recover time [" + haveTryTime + "]", e);
                    sleepMS(tryBackTimeMS);
                    message = "reconnect";
                } else {
                    log.error("KafkaRecordFetcher: unrecoverable error  " + e.getMessage() + ", have try time [" + haveTryTime + "]", e);
                    consumerContext.exit();
                }
            } finally {
                swallowErrorClose(kafkaConsumerWrap);
            }
        }
    }

    private boolean offerRecord(int timeOut, TimeUnit timeUnit, ConsumerRecord<byte[],byte[]> record) {
        try {
            recordStoreInCountSensor.record(1);
            recordStoreInByteSensor.record(record.value().length);
            return toProcessRecord.offer(record, timeOut, timeUnit);
        } catch (Exception e) {
            log.error("UserRecordGenerator: offer record failed, record[" + record + "], cause " + e.getMessage(), e);
            return false;
        }
    }

    private boolean isErrorRecoverable(Throwable e) {
        if (e instanceof TimestampSeekException) {
            return false;
        } else {
            return true;
        }
    }

    private ConsumerWrap getConsumerWrap(String message) {
        ConsumerWrap kafkaConsumerWrap = getConsumerWrap();
        Checkpoint checkpoint = null;
        metaStoreCenter.registerStore(KAFKA_STORE_NAME, new KafkaMetaStore(kafkaConsumerWrap.getRawConsumer()));

        if (useCheckpointConfig.compareAndSet(true, false)) {
            log.info("RecordGenerator: force use initial checkpoint [{}] to start", checkpoint);
            checkpoint = initialCheckpoint;
        } else {
            checkpoint = getCheckpoint();
            if (null == checkpoint || Checkpoint.INVALID_STREAM_CHECKPOINT == checkpoint) {
                checkpoint = initialCheckpoint;
                log.info("RecordGenerator: use initial checkpoint [{}] to start", checkpoint);
            } else {
                log.info("RecordGenerator: load checkpoint from checkpoint store success, current checkpoint [{}]", checkpoint);
            }
        }

        switch (subscribeMode) {
            case SUBSCRIBE: {
                kafkaConsumerWrap.subscribeTopic(topicPartition, () -> {
                    Checkpoint ret = getSubscribeCheckpoint();

                    if (null == ret || Checkpoint.INVALID_STREAM_CHECKPOINT == ret) {
                        log.info("Subscribe checkpoint is null, use initialCheckpoint: " + initialCheckpoint);

                        ret = initialCheckpoint;
                    }
                    return ret;
                });
                break;
            }
            case ASSIGN:{
                kafkaConsumerWrap.assignTopic(topicPartition, checkpoint);
                break;
            }
            default: {
                throw new RuntimeException("RecordGenerator: unknown mode not support");
            }
        }

        //seek checkpoint firstly
        //kafkaConsumerWrap.setFetchOffsetByTimestamp(topicPartition, checkpoint);

        log.info("RecordGenerator:" + message + ", checkpoint " + checkpoint);
        return kafkaConsumerWrap;
    }

    private ConsumerWrap getConsumerWrap() {
        Properties properties = consumerContext.getKafkaProperties();

        return new ConsumerWrap.DefaultConsumerWrap(properties, consumerContext);
    }

    private Checkpoint getCheckpoint() {
        // use local checkpoint priority
        Checkpoint checkpoint = metaStoreCenter.seek(composeLocalFileStoreName(LOCAL_FILE_STORE_NAME, groupID), topicPartition, groupID);
        if (null == checkpoint) {
            checkpoint = metaStoreCenter.seek(KAFKA_STORE_NAME, topicPartition, groupID);
        }

        if (null == checkpoint) {
            checkpoint = metaStoreCenter.seek(USER_STORE_NAME, topicPartition, groupID);

            log.info("DStore checkpoint is null, load checkpoint from user defined shared store: " + checkpoint);
        }
        return checkpoint;
    }

    private Checkpoint getSubscribeCheckpoint() {
        Checkpoint checkpoint = metaStoreCenter.seek(KAFKA_STORE_NAME, topicPartition, groupID);
        log.info("Load checkpoint from DStore: " + checkpoint);

        if (null == checkpoint) {
            checkpoint = metaStoreCenter.seek(USER_STORE_NAME, topicPartition, groupID);

            log.info("DStore checkpoint is null, load checkpoint from user defined shared store: " + checkpoint);
        }
        return checkpoint;
    }

    private void mayCommitCheckpoint() {
        if (null != toCommitCheckpoint && System.currentTimeMillis() >= nextCommitTimestamp) {
            commitCheckpoint(toCommitCheckpoint.getTopicPartition(), toCommitCheckpoint);
            toCommitCheckpoint = null;
            nextCommitTimestamp = System.currentTimeMillis() + consumerContext.getCheckpointCommitInterval();
        }
    }

    public void commitCheckpoint(TopicPartition topicPartition, Checkpoint checkpoint) {
        if (null != topicPartition && null != checkpoint) {
            metaStoreCenter.store(topicPartition, groupID, checkpoint);
        }
    }

    public void setToCommitCheckpoint(Checkpoint committedCheckpoint) {
        this.toCommitCheckpoint = committedCheckpoint;
    }

    private String composeLocalFileStoreName(String prefix, String sid) {
        return StringUtils.join(prefix, "-", sid);
    }

    @Override
    public void close() throws IOException {
        consumerContext.exit();
    }
}
