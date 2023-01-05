package com.taobao.drc.client.network.dstore;

import com.aliyun.dts.subscribe.clients.recordfetcher.ClusterSwitchListener;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.network.DataFlowLimitHandler;
import com.taobao.drc.client.network.RecordNotifyHelper;
import com.taobao.drc.client.network.RecordPreNotifyHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * A abstract class that using to consume data from DStore,which implements framework method.
 * @author xusheng.zkw
 */
public abstract class BaseDStoreConsumer extends Thread implements DStoreConsumer {
    private static final Log logger = LogFactory.getLog(BaseDStoreConsumer.class);
    protected UserConfig config;
    protected final Listener listener;
    protected int pollTimeOut = 200;
    protected RecordPreNotifyHelper preHelper;
    protected RecordNotifyHelper notifyHeper;
    protected TopicPartition assignedPartition;
    private DataFlowLimitHandler dataFlowLimitHandler;

    private final CountDownLatch sync = new CountDownLatch(1);

    private Throwable error;
    private volatile boolean isClosed;

    public BaseDStoreConsumer(Listener listener) {
        this.listener = listener;
    }

    abstract List<DataMessage.Record> poll(long timeoutMs) throws InterruptedException;
    abstract void doClose();
    abstract Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestamp);
    abstract Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);

    @Override
    public void init(UserConfig userConfig, CheckpointManager checkpointManager) {
        this.config = userConfig;
        pollTimeOut = Integer.parseInt(userConfig.getPollTimeoutMs());
        preHelper = new RecordPreNotifyHelper(userConfig);
        notifyHeper = new RecordNotifyHelper(userConfig, checkpointManager, listener);
        dataFlowLimitHandler = new DataFlowLimitHandler(this.config);
    }

    @Override
    public UserConfig getConfig() {
        return config;
    }

    /**
     * A thread to  consume data from Dtore.
     */
    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                //listener notify
                List<DataMessage.Record> records = poll(pollTimeOut);
                if (null != records && !records.isEmpty()) {
                    for (DataMessage.Record record : records) {
                        dataFlowLimitHandler.channelRead();
                        preHelper.process(record);
                        notifyHeper.process(record);
                    }
                }
            } catch (InterruptedException e) {
                sync.countDown();
            } catch (org.apache.kafka.common.errors.InterruptException e) {
                sync.countDown();
            }  catch (Throwable e) {
                logger.error("poll record error", e);
                sync.countDown();
                throw new RuntimeException(e);
            }
        }
        sync.countDown();
    }

    /**
     * Block the consuming data thread, until sync object is countdown.
     */
    @Override
    public void sync() {
        try {
            sync.await();
        } catch (InterruptedException e) {
            logger.warn("[" + config.getSubTopic() + "]leave sync method.");
        }
    }

    @Override
    public void close() {
        try {
            doClose();
        } catch (Throwable e) {
            logger.warn("close DStoreConsumer failed." + e.getMessage());
        }
        this.interrupt();
    }

    protected Map<String, Object> consumerConfig(UserConfig userConfig) {
        Map<String, Object> cfg = new HashMap<String, Object>();
        String jobId = UUID.randomUUID().toString();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, userConfig.getChannelUrl());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, jobId);
        cfg.put(ConsumerConfig.CLIENT_ID_CONFIG, getClientId("consumer", jobId));
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        if (StringUtils.isNumeric(userConfig.getPollBytesSize())) {
            cfg.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.parseInt(userConfig.getPollBytesSize()));
        } else {
            cfg.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024 * 1024 * 2);
        }

        if (StringUtils.isNumeric(userConfig.getPollRecordsSize())) {
            cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.parseInt(userConfig.getPollRecordsSize()));
        } else {
            cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
        }

        if (StringUtils.isNumeric(userConfig.getFetchWaitMs())) {
            cfg.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Integer.parseInt(userConfig.getFetchWaitMs()));
        } else {
            cfg.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200);
        }
        cfg.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, cfg.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
        cfg.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 5 * (Integer) cfg.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        cfg.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 2 * (Integer) cfg.get(ConsumerConfig.FETCH_MAX_BYTES_CONFIG));
        cfg.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
        cfg.put("sasl.mechanism", StringUtils.isEmpty(userConfig.getSaslMechanism()) ? "PLAIN" : userConfig.getSaslMechanism());
        cfg.put("security.protocol", StringUtils.isEmpty(userConfig.getSecurityProtocal()) ? "SASL_PLAINTEXT" : userConfig.getSecurityProtocal());
        cfg.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required\n" +
                "  dts.api.url=vvv\n" +
                "  username=\"" + userConfig.getUserName() + "-" + userConfig.getDtsChannelId() + "\"\n" +
                "  password=\"" + userConfig.getPassword() + "\";");

        // to let the consumer feel the switch of cluster and reseek the offset by timestamp
        //cfg.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ClusterSwitchListener.class.getName());
        return cfg;
    }

    /**
     * Using timestamp position to fetch kafka offset.
     * @return
     */
    protected long getPosition() {
        String position = config.getCheckpoint().getTimestamp();
        Map<TopicPartition, Long> timestamp = new HashMap<TopicPartition, Long>();
        long requestedTime = Long.valueOf(position);
        timestamp.put(assignedPartition, requestedTime);
        Map<TopicPartition, OffsetAndTimestamp> result = offsetsForTimes(timestamp);
        if (result == null) {
            throw new DStoreOffsetNotExistException("Cannot get offset for timestamp: " + position);
        }
        OffsetAndTimestamp offsetAndTimestamp = result.get(assignedPartition);
        logger.info("getPosition for timestamp: " + position + ", offset and timestap:" + offsetAndTimestamp);
        if (offsetAndTimestamp == null) {
            throw new DStoreOffsetNotExistException("Cannot get offset for timestamp: " + position + " on topic=" + assignedPartition.topic());
        }
        if (offsetAndTimestamp.timestamp() != requestedTime
                && offsetAndTimestamp.offset()<=beginningOffsets(Arrays.asList(assignedPartition)).get(assignedPartition)) {
            throw new DStoreOffsetNotExistException("Requested Timestamp not exist.");
        }
        return offsetAndTimestamp.offset();
    }

    private static String getClientId(String role, String jobId) {
        String localIp = "";
        try {
            localIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException var4) {
        }
        return String.format("%s-%s-%s", role, jobId, localIp);
    }
}
