package com.taobao.drc.client.network;

import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.network.dstore.DStoreConsumer;
import com.taobao.drc.client.network.dstore.SubscriptionConsumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DStore network endpoint
 * @author xusheng.zkw
 */
public class DStoreNetworkEndpoint {
    private static final Logger log = LoggerFactory.getLogger(DStoreNetworkEndpoint.class);
    private final Map<String, DStoreConsumer> channels = new ConcurrentHashMap<String, DStoreConsumer>();
    private Listener listener;
    private final Object lifecycleLock = new Object();
    private long closeGeneration;

    public void setMessageListener(Listener listener) {
        this.listener = listener;
    }



    /**
     * this method will blocking caller thread.
     * @param userConfig
     */
    public void connectToStoreThenWait(UserConfig userConfig, CheckpointManager checkpointManager) {
        connectToStoreThenWait(userConfig, checkpointManager, getGeneration());
    }

    /** Capture before CM registration, so a stop during registration fences startup. */
    public long getGeneration() {
        synchronized (lifecycleLock) { return closeGeneration; }
    }

    public void connectToStoreThenWait(UserConfig userConfig, CheckpointManager checkpointManager, long generation) {
        if (!StringUtils.isNumeric(userConfig.getPollTimeoutMs())) {
            userConfig.setPollTimeoutMs("200");
        }
        final DStoreConsumer previous;
        synchronized (lifecycleLock) {
            if (generation != closeGeneration || Thread.currentThread().isInterrupted()) { return; }
            previous = channels.remove(userConfig.getSubTopic());
        }
        if (previous != null) {
            previous.close();
            awaitTermination(previous);
        }
        DStoreConsumer consumer = createConsumer();
        try {
            consumer.init(userConfig, checkpointManager);
            synchronized (lifecycleLock) {
                if (generation != closeGeneration || Thread.currentThread().isInterrupted()) { return; }
                channels.put(userConfig.getSubTopic(), consumer);
                consumer.start();
            }
            consumer.sync();
        } finally {
            consumer.close();
            awaitTermination(consumer);
            channels.remove(userConfig.getSubTopic(), consumer);
        }
    }

    protected DStoreConsumer createConsumer() { return new SubscriptionConsumer(listener); }

    public void close() {
        List<DStoreConsumer> closing;
        synchronized (lifecycleLock) {
            closeGeneration++;
            closing = new ArrayList<DStoreConsumer>(channels.values());
            channels.clear();
        }
        boolean calledFromConsumer = com.taobao.drc.client.impl.ListenerContext.isCallbackThread();
        for (DStoreConsumer consumer : closing) {
            log.info("close channel [" + consumer.getConfig().getSubTopic() + "]");
            consumer.close();
            calledFromConsumer |= consumer == Thread.currentThread();
        }
        // A listener may stop the client from its own worker. Never join other
        // workers in that callback: concurrent callbacks could wait on each other.
        if (!calledFromConsumer) {
            for (DStoreConsumer consumer : closing) { awaitTermination(consumer); }
        }
    }

    private void awaitTermination(DStoreConsumer consumer) {
        if (!(consumer instanceof Thread) || consumer == Thread.currentThread()) { return; }
        boolean interrupted = false;
        Thread worker = (Thread) consumer;
        while (worker.isAlive()) {
            try { worker.join(); }
            catch (InterruptedException e) { interrupted = true; }
        }
        if (interrupted) { Thread.currentThread().interrupt(); }
    }
}
