package com.aliyun.dts.subscribe.clients.recordgenerator;

import com.aliyun.dts.subscribe.clients.ConsumerContext;
import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;
import com.aliyun.dts.subscribe.clients.common.WorkThread;
import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import com.aliyun.dts.subscribe.clients.recordfetcher.OffsetCommitCallBack;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.SimpleRate;
import org.apache.kafka.common.metrics.stats.Total;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.aliyun.dts.subscribe.clients.common.Util.sleepMS;

/**
 * This class is to resolve avro record deserialize from bytes to UserRecord
 */
public class UserRecordGenerator implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(UserRecordGenerator.class);

    static {
        System.setProperty("kafka.metrics.reporters", "org.apache.kafka.common.metrics.JmxReporter");
    }

    protected ConsumerContext consumerContext;
    protected final LinkedBlockingQueue<ConsumerRecord> toProcessRecord;
    protected final AvroDeserializer fastDeserializer;

    protected final LinkedBlockingQueue<DefaultUserRecord> processedRecord;

    protected volatile Checkpoint commitCheckpoint;
    protected WorkThread commitThread;
    protected final OffsetCommitCallBack offsetCommitCallBack;

    protected Metrics metrics;

    protected final Sensor recordStoreOutCountSensor;
    protected final Sensor recordStoreOutByteSensor;

    public UserRecordGenerator(ConsumerContext consumerContext, LinkedBlockingQueue<ConsumerRecord> toProcessRecord, LinkedBlockingQueue<DefaultUserRecord> processedRecord,
                               OffsetCommitCallBack offsetCommitCallBack) {
        this.consumerContext = consumerContext;
        this.toProcessRecord = toProcessRecord;
        this.fastDeserializer = new AvroDeserializer();
        this.processedRecord = processedRecord;

        this.offsetCommitCallBack = offsetCommitCallBack;

        commitCheckpoint = new Checkpoint(null, -1, -1, "-1");

        metrics = consumerContext.getDtsMetrics().getCoreMetrics();

        metrics.addMetric(
                metrics.metricName("DStoreRecordQueue", "UserRecordGenerator"),
                (config, now) -> (toProcessRecord.size()));

        metrics.addMetric(
                metrics.metricName("DefaultUserRecordQueue", "UserRecordGenerator"),
                (config, now) -> (processedRecord.size()));

        this.recordStoreOutCountSensor = metrics.sensor("record-store-out-row");
        this.recordStoreOutCountSensor.add(metrics.metricName("outCounts", "recordstore"), new Total());
        this.recordStoreOutCountSensor.add(metrics.metricName("outRps", "recordstore"), new SimpleRate());
        this.recordStoreOutByteSensor = metrics.sensor("record-store-out-byte");
        this.recordStoreOutByteSensor.add(metrics.metricName("outBytes", "recordstore"), new Total());
        this.recordStoreOutByteSensor.add(metrics.metricName("outBps", "recordstore"), new SimpleRate());
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

                DefaultUserRecord defaultUserRecord = new DefaultUserRecord(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset(),
                        record,
                        (tp, commitRecord, offset, metadata) -> {
                            recordStoreOutCountSensor.record(1);
                            recordStoreOutByteSensor.record(consumerRecord.value().length);
                            commitCheckpoint = new Checkpoint(tp, commitRecord.getSourceTimestamp(), offset, metadata);
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

    protected boolean offerRecord(int timeOut, TimeUnit timeUnit, DefaultUserRecord defaultUserRecord) {
        try {
            return processedRecord.offer(defaultUserRecord, timeOut, timeUnit);
        } catch (Exception e) {
            log.error("UserRecordGenerator: offer record failed, record[" + defaultUserRecord + "], cause " + e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        consumerContext.exit();
        commitThread.stop();
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
