package com.aliyun.dms.subscribe.clients;

import com.aliyun.dts.subscribe.clients.ConsumerContext;
import com.aliyun.dts.subscribe.clients.DTSConsumer;
import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.metastore.MetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DefaultDistributedDTSConsumer implements DistributedDTSConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDistributedDTSConsumer.class);
    private List<DTSConsumer> dtsConsumers = new ArrayList<>();


    private int corePoolSize = 8;
    private int maximumPoolSize = 64;
    private ThreadPoolExecutor executor;
    private volatile boolean isClosePoolExecutor = false;

    public DefaultDistributedDTSConsumer() {}

    public void addDTSConsumer(DTSConsumer consumer) {
        dtsConsumers.add(consumer);
    }

    public void init(Map<String, String> topic2checkpoint, DBMapper dbMapper, String dProxy, Map<String, String> topic2Sid, String username, String password,
                     ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint,
                     MetaStore<Checkpoint> userRegisteredStore, Map<String, RecordListener> recordListeners) {

        this.init(topic2checkpoint, dbMapper, dProxy, topic2Sid, username, password, subscribeMode,
                isForceUseInitCheckpoint, userRegisteredStore, recordListeners, new Properties());
    }

    public void init(Map<String, String> topic2checkpoint, DBMapper dbMapper, String dProxy, Map<String, String> topic2Sid, String username, String password,
                     ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint,
                     MetaStore<Checkpoint> userRegisteredStore, Map<String, RecordListener> recordListeners, Properties properties) {

        this.executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 1000 * 60,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        for (Map.Entry<String, String> topicCheckpoint: topic2checkpoint.entrySet()) {

            ConsumerContext consumerContext = new ConsumerContext(dbMapper, dProxy, topicCheckpoint.getKey(), topic2Sid.get(topicCheckpoint.getKey()), username, password,
                    topicCheckpoint.getValue(), subscribeMode, properties);
            consumerContext.setUserRegisteredStore(userRegisteredStore);
            consumerContext.setForceUseCheckpoint(isForceUseInitCheckpoint);

            DTSConsumer dtsConsumer = new DTSConsumerWithDBMapping(consumerContext);
            dtsConsumer.addRecordListeners(recordListeners);

            addDTSConsumer(dtsConsumer);
        }
    }
    @Override
    public void start() {
        for (DTSConsumer consumer: dtsConsumers) {
            try {
                executor.submit(consumer::start);
            } catch (Exception e) {
                LOG.error("error starting consumer:" + e);
                shutdownGracefully(10, TimeUnit.SECONDS);
            }
        }
    }

    public void shutdownGracefully(long timeout, TimeUnit timeUnit) {
        executor.shutdown();

        try {
            if (!executor.awaitTermination(timeout, timeUnit)) {
                executor.shutdownNow();

            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            isClosePoolExecutor = true;
        }
    }

    @Override
    public void addRecordListeners(Map<String, RecordListener> recordListeners) {
        for (DTSConsumer dtsConsumer: dtsConsumers) {
            dtsConsumer.addRecordListeners(recordListeners);
        }
    }

}
