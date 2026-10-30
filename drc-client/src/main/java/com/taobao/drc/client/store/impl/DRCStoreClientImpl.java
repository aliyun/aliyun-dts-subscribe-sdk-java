package com.taobao.drc.client.store.impl;

import com.taobao.drc.client.DRCClientException;
import com.taobao.drc.client.DataFilterBase;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.cm.ClusterManagerFacade;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.TransportType;
import com.taobao.drc.client.network.ConnectionStateChangeListener;
import com.taobao.drc.client.network.DRCClientThreadFactory;
import com.taobao.drc.client.network.NetworkEndpoint;
import com.taobao.drc.client.utils.NetworkUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implement the StoreClient, using ManagerCommunicator and ServerCommunicator
 * to connect with manager and daemon server respectively, receive data, parse
 * and notify listeners.
 */
public class DRCStoreClientImpl extends AbstractStoreClient {
    private static final Logger log = LoggerFactory.getLogger(DRCStoreClientImpl.class);
    private final NetworkEndpoint endpoint;
    private volatile ExecutorService service;
    private final AtomicBoolean firstBootstrap = new AtomicBoolean(false);

    public DRCStoreClientImpl(Listener listener, UserConfig userConfig, CheckpointManager checkpointManager, DataFilterBase filter, String parseThreadPrefix, String notifyThreadPrefix) {
        super(listener, userConfig, checkpointManager, filter, parseThreadPrefix, notifyThreadPrefix);
        checkpointManager.setMaxPendingAckRanges(userConfig.getMaxPendingAckRanges());
        endpoint = new NetworkEndpoint();
        endpoint.setTransportType(userConfig.getTransportType());
        endpoint.setMessageListener(listener);
        //set thread factory prefix
        if(parseThreadPrefix!=null){
            endpoint.setParseFactory(new DRCClientThreadFactory(parseThreadPrefix));
        }

        if(notifyThreadPrefix!=null){
            endpoint.setNotifyFactory(new DRCClientThreadFactory(notifyThreadPrefix));
        }
        endpoint.setSqlEngine(userConfig.getEngine());
    }

    private void validateDataFilter(String subTopic) throws IOException, DRCClientException {
        if (null == dbType) {
            dbType = NetworkUtils.retrieveDBTypeFromRemote(userConfig, subTopic);
            //parse filter
            if( false == filter.validateFilter(dbType)) {
                throw new DRCClientException("Data filter check failed: for oceanbase 1.0, the filter format must be " +
                        " [tenant.dbname.tbname.clos]");
            }
        }
    }

    /**
     * The method return - Callable will block caller thread.
     * Send request to daemon server and get data continuously. The method will
     * not be blocked after the thread starts.
     * @return
     * @throws Exception
     */
    @Override
    public Callable<Boolean> startService() throws Exception {
        boolean firstBoot = firstBootstrap.compareAndSet(false, true);
        if (firstBoot) {
            endpoint.start(false, userConfig);
        }
        final long generation = endpoint.getGeneration();
        ClusterManagerFacade.askToken(userConfig);
        ClusterManagerFacade.askTopic(userConfig);
        ClusterManagerFacade.askRegionInfo(userConfig);
        prepareSingleTopic(userConfig.getSubTopic());
        validateDataFilter(userConfig.getSubTopic());
        userConfig.getCheckpoint().setTimestamp(checkpointManager.prepareDrcRecovery(userConfig.getCheckpoint().getTimestamp()));
        final ClusterManagerFacade.StoreInfo storeInfo = ClusterManagerFacade.fetchStoreInfo(userConfig,
                userConfig.getTransportType() == TransportType.DRCNET);
        Callable<Boolean> runnable = new Callable() {
            @Override
            public Boolean call() throws Exception {
                boolean callResult = false;
                try {
                    NonReconnectableConnectionStateChangeListener stateListener = new NonReconnectableConnectionStateChangeListener(userConfig);
                    ChannelFuture channelFuture = endpoint.connectToStore(storeInfo, userConfig, stateListener, checkpointManager, generation);
                    log.info("DRCClient start,subTopic:" + userConfig.getSubTopic() + " init checkpoint:" + userConfig.getCheckpoint().toString());
                    // block until channel is inactive
                    stateListener.blockUntilDisconnected();
                    if (stateListener.isGottenFirstRecord()) {
                        callResult = true;
                    }
                    // block until channel is closed
                    channelFuture.channel().closeFuture().sync();
                    rethrow(stateListener.getThrowable());
                } catch (Exception e) {
                    log.error("client is error", e);
                    throw e;
                }
                return callResult;
            }
        };
        return runnable;
    }

    /**
     * The method return - Callable will block caller thread.
     * @return
     */
    @Override
    public Callable<Boolean> startMultiService() {
        boolean firstBoot = firstBootstrap.compareAndSet(false, true);
        if (firstBoot) {
            endpoint.start(true, userConfig);
        }
        final long generation = endpoint.getGeneration();
        Callable<Boolean> runnable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                ClusterManagerFacade.askToken(userConfig);
                ClusterManagerFacade.askRegionInfo(userConfig);
                List<String> subTopics = ClusterManagerFacade.askMultiTopic(userConfig);
                if (subTopics.isEmpty()) { throw new IllegalStateException("No DRC subscription topics"); }
                validateDataFilter(subTopics.get(0));
                prepareTopics(subTopics);
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger readRecordCount = new AtomicInteger();
                final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
                final ExecutorService attemptService = createMultiServiceExecutor(subTopics.size(), "DRCMultiServiceDaemon");
                service = attemptService;
                try {
                    for (final String subTopic : subTopics) {
                        attemptService.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    UserConfig uc = userConfigMap.get(subTopic);
                                    CheckpointManager cm = checkpointManagerMap.get(subTopic);
                                    ClusterManagerFacade.askToken(uc);
                                    if (Thread.currentThread().isInterrupted() || endpoint.getGeneration() != generation) { return; }
                                    uc.getCheckpoint().setTimestamp(cm.prepareDrcRecovery(uc.getCheckpoint().getTimestamp()));
                                    ClusterManagerFacade.StoreInfo storeInfo = ClusterManagerFacade.fetchStoreInfo(uc, userConfig.getTransportType() == TransportType.DRCNET);
                                    NonReconnectableConnectionStateChangeListener stateListener = new NonReconnectableConnectionStateChangeListener(uc);
                                    ChannelFuture future = endpoint.connectToStore(storeInfo, uc, stateListener, cm, generation);
                                    log.info("DRCClient start,subTopic:" + uc.getSubTopic() + " init checkpoint:" + uc.getCheckpoint().toString());
                                    stateListener.blockUntilDisconnected();
                                    if (stateListener.isGottenFirstRecord()) {
                                        readRecordCount.incrementAndGet();
                                    }
                                    future.channel().closeFuture().sync();
                                    rethrow(stateListener.getThrowable());
                                } catch (Throwable e) {
                                    failure.compareAndSet(null, e);
                                    log.info("client start failed,subTopic:" + subTopic + ",error:[" + e.getMessage() + "]");
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
                    firstBootstrap.set(false);
                    boolean interrupted = false;
                    while (!attemptService.isTerminated()) {
                        try { attemptService.awaitTermination(1, TimeUnit.DAYS); }
                        catch (InterruptedException e) { interrupted = true; }
                    }
                    for (CheckpointManager manager : checkpointManagerMap.values()) { manager.invalidate(); }
                    if (interrupted) { Thread.currentThread().interrupt(); }
                }
                rethrow(failure.get());
                return readRecordCount.get() == subTopics.size();
            }
        };
        return runnable;
    }

    @Override
    public Future writeUserCtlMessage(byte[] userCtlBytes) {
        if (endpoint == null) {
            throw new IllegalStateException("DRCClient.initService not called yet!");
        }
        return endpoint.writeUserCtlMessage(userCtlBytes);
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
            log.warn("shutdown DRCClient failed." + e.getMessage());
        } finally {
            firstBootstrap.compareAndSet(true, false);
        }
    }

    private static void rethrow(Throwable cause) throws Exception {
        if (cause instanceof Exception) { throw (Exception) cause; }
        if (cause instanceof Error) { throw (Error) cause; }
        if (cause != null) { throw new IllegalStateException("DRC shard failed", cause); }
    }

    private static class NonReconnectableConnectionStateChangeListener implements ConnectionStateChangeListener {
        private final UserConfig userConfig;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private volatile boolean disconnected;
        private volatile boolean gottenFirstRecord;
        private volatile Throwable throwable;

        public NonReconnectableConnectionStateChangeListener(UserConfig userConfig) {
            this.userConfig = userConfig;
        }

        @Override
        public void onChannelActive(Channel channel) {
            log.info("Channel of [" + userConfig.getSubTopic() + "] to [" + channel.remoteAddress() + "] is active");
        }

        @Override
        public void onFirstRecord(Channel channel) {
            gottenFirstRecord = true;
            log.info("Got first record on channel of [" + userConfig.getSubTopic() + "] to [" + channel.remoteAddress() + "]");
        }

        private void notifyDisconnection() {
            disconnected = true;
            lock.lock();
            try {
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public long onChannelInactive(Channel channel) {
            notifyDisconnection();
            log.warn("Channel of [" + userConfig.getSubTopic() + "] to [" + channel.remoteAddress() + "] is inactive");
            return -1;
        }

        @Override
        public synchronized long onException(Channel channel, Throwable throwable) {
            if (this.throwable == null) { this.throwable = throwable; }
            log.warn("Caught exception on channel of [" + userConfig.getSubTopic() + "]" +
                            (channel == null ? "" : ", address[" + channel.remoteAddress() + "]"),
                    throwable);
            notifyDisconnection();
            return -1;
        }

        public boolean isGottenFirstRecord() {
            return gottenFirstRecord;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        public void blockUntilDisconnected() throws InterruptedException {
            lock.lock();
            try { while (!disconnected) { condition.await(); } }
            finally { lock.unlock(); }
        }
    }
}
