package com.taobao.drc.client.store.impl;

import com.taobao.drc.client.DataFilterBase;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.cm.ClusterManagerFacade;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.network.DStoreNetworkEndpoint;
import com.taobao.drc.client.network.dstore.DStoreOffsetNotExistException;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

public class DStoreClientImpl  extends AbstractStoreClient {
    private static final Logger log = LoggerFactory.getLogger(DStoreClientImpl.class);
    private volatile ExecutorService service;
    private final DStoreNetworkEndpoint endpoint;
    public DStoreClientImpl(Listener listener, UserConfig userConfig, CheckpointManager checkpointManager, DataFilterBase filter, String parseThreadPrefix, String notifyThreadPrefix) {
        super(listener, userConfig, checkpointManager, filter, parseThreadPrefix, notifyThreadPrefix);
        checkpointManager.setMaxPendingAckRanges(userConfig.getMaxPendingAckRanges());
        endpoint = new DStoreNetworkEndpoint();
        endpoint.setMessageListener(listener);
    }

    @Override
    public Future writeUserCtlMessage(byte[] userCtlBytes) {
        return null;
    }

    @Override
    public void stopService() {
        checkpointManager.invalidate();
        for (CheckpointManager manager : checkpointManagerMap.values()) { manager.invalidate(); }
        try {
            ExecutorService stopping = service;
            if (stopping != null) { stopping.shutdownNow(); }
            if (null != endpoint) {
                endpoint.close();
            }
        } catch (Throwable e) {
            log.warn("shutdown DStoreClient failed." + e.getMessage());
        }
    }

    @Override
    public Callable<Boolean> startService() {
        final long generation = endpoint.getGeneration();
        Callable<Boolean> runnable = new Callable() {
            @Override
            public Boolean call() {
                String[] topics = ClusterManagerFacade.askForDStoreTopic(userConfig);
                if (null == topics || topics.length < 1) {
                    throw new RuntimeException("There is no topic exist.");
                }
                boolean callResult = false;
                try {
                    log.info("DTSClient start, subTopic:" + topics[0]);
                    prepareSingleTopic(topics[0]);
                    //register channelId
                    String channelId = ClusterManagerFacade.enrollDStoreChannel(userConfig,
                            checkpointManager.prepareRecovery(userConfig.getCheckpoint().getTimestamp()));
                    //kafka
                    userConfig.setDtsChannelId(channelId);
                    //block thread
                    endpoint.connectToStoreThenWait(userConfig, checkpointManager, generation);
                    callResult = true;
                } catch (DStoreOffsetNotExistException e) {
                    throw e;
                } catch (Throwable e) {
                    log.info("DTSClient start failed,subTopic:" + topics[0] + ",error:[" + e.getMessage() + "]");
                    throw e;
                }
                return callResult;
            }
        };
        return runnable;
    }

    @Override
    public Callable<Boolean> startMultiService() {
        final long generation = endpoint.getGeneration();
        Callable<Boolean> runnable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                String[] topics = ClusterManagerFacade.askForDStoreTopic(userConfig);
                if (null == topics || topics.length < 1) {
                    throw new RuntimeException("There is no topic exist.");
                }
                prepareTopics(java.util.Arrays.asList(topics));
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger readRecordCount = new AtomicInteger();
                final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
                final ExecutorService attemptService = createMultiServiceExecutor(topics.length, "DTSMultiServiceDaemon");
                service = attemptService;
                try {
                    for (final String topic : topics) {
                        attemptService.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    if (Thread.currentThread().isInterrupted() || endpoint.getGeneration() != generation) { return; }
                                    UserConfig uc = userConfigMap.get(topic);
                                    CheckpointManager cm = checkpointManagerMap.get(topic);
                                    log.info("DTSClient start, subTopic:" + topic);
                                    //register channelId
                                    String channelId = ClusterManagerFacade.enrollDStoreChannel(uc,
                                            cm.prepareRecovery(uc.getCheckpoint().getTimestamp()));
                                    //kafka
                                    uc.setDtsChannelId(channelId);
                                    //block thread
                                    endpoint.connectToStoreThenWait(uc, cm, generation);
                                    readRecordCount.incrementAndGet();
                                } catch (Throwable e) {
                                    failure.compareAndSet(null, e);
                                    log.info("DTSClient start failed,subTopic:" + topic + ",error:[" + e.getMessage() + "]");
                                } finally {
                                    latch.countDown();
                                }
                            }
                        });
                    }
                    latch.await();
                } finally {
                    for (CheckpointManager manager : checkpointManagerMap.values()) { manager.invalidate(); }
                    attemptService.shutdownNow();
                    endpoint.close();
                    boolean interrupted = false;
                    while (!attemptService.isTerminated()) {
                        try { attemptService.awaitTermination(1, TimeUnit.DAYS); }
                        catch (InterruptedException e) { interrupted = true; }
                    }
                    for (CheckpointManager manager : checkpointManagerMap.values()) { manager.invalidate(); }
                    if (interrupted) { Thread.currentThread().interrupt(); }
                }
                Throwable cause = failure.get();
                if (cause instanceof Exception) { throw (Exception) cause; }
                if (cause instanceof Error) { throw (Error) cause; }
                if (cause != null) { throw new IllegalStateException("DTS shard failed", cause); }
                return readRecordCount.get() == topics.length;
            }
        };
        return runnable;
    }
}
