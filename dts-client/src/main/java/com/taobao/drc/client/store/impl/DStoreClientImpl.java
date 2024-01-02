package com.taobao.drc.client.store.impl;

import com.taobao.drc.client.DataFilterBase;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.cm.ClusterManagerFacade;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.network.DRCClientThreadFactory;
import com.taobao.drc.client.network.DStoreNetworkEndpoint;
import com.taobao.drc.client.network.dstore.DStoreOffsetNotExistException;
import io.netty.util.concurrent.Future;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class DStoreClientImpl  extends AbstractStoreClient {
    private static final Logger log = LoggerFactory.getLogger(DStoreClientImpl.class);
    private volatile ExecutorService service;
    private final DStoreNetworkEndpoint endpoint;
    public DStoreClientImpl(Listener listener, UserConfig userConfig, CheckpointManager checkpointManager, DataFilterBase filter, String parseThreadPrefix, String notifyThreadPrefix) {
        super(listener, userConfig, checkpointManager, filter, parseThreadPrefix, notifyThreadPrefix);
        endpoint = new DStoreNetworkEndpoint();
        endpoint.setMessageListener(listener);
    }

    @Override
    public Future writeUserCtlMessage(byte[] userCtlBytes) {
        return null;
    }

    @Override
    public void stopService() {
        try {
            if (null != service) {
                service.shutdownNow();
                service = null;
            }
            if (null != endpoint) {
                endpoint.close();
            }
        } catch (Throwable e) {
            log.warn("shutdown DStoreClient failed." + e.getMessage());
        }
    }

    @Override
    public Callable<Boolean> startService() {
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
                    userConfig.setSubTopic(topics[0]);
                    //register channelId
                    String channelId = ClusterManagerFacade.enrollDStoreChannel(userConfig);
                    //kafka
                    userConfig.setDtsChannelId(channelId);
                    //block thread
                    endpoint.connectToStoreThenWait(userConfig, checkpointManager);
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
        Callable<Boolean> runnable = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                String[] topics = ClusterManagerFacade.askForDStoreTopic(userConfig);
                if (null == topics || topics.length < 1) {
                    throw new RuntimeException("There is no topic exist.");
                }
                final CountDownLatch latch = new CountDownLatch(1);
                final AtomicInteger readRecordCount = new AtomicInteger();
                service = Executors.newFixedThreadPool(topics.length, new DRCClientThreadFactory("DTSMultiServiceDaemon"));
                for (final String topic : topics) {
                    service.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                UserConfig uc;
                                if (!userConfigMap.containsKey(topic)) {
                                    uc = (UserConfig) BeanUtils.cloneBean(userConfig);
                                    uc.setMultiMode(true);
                                    uc.setSubTopic(topic);
                                    /**
                                     * inner deep clone
                                     */
                                    cloneInner(userConfig, uc);
                                    userConfigMap.put(topic, uc);

                                    //checkpoint
                                    CheckpointManager checkpointManager = new CheckpointManager(userConfig.isMultiMode());
                                    checkpointManager.setMultiMode(true);
                                    checkpointManagerMap.put(topic, checkpointManager);
                                } else {
                                    uc = userConfigMap.get(topic);
                                }
                                log.info("DTSClient start, subTopic:" + topic);
                                //register channelId
                                String channelId = ClusterManagerFacade.enrollDStoreChannel(uc);
                                //kafka
                                uc.setDtsChannelId(channelId);
                                //block thread
                                endpoint.connectToStoreThenWait(uc, checkpointManager);
                                readRecordCount.incrementAndGet();
                            } catch (DStoreOffsetNotExistException e) {
                                throw e;
                            } catch (Throwable e) {
                                log.info("DTSClient start failed,subTopic:" + topic + ",error:[" + e.getMessage() + "]");
                            } finally {
                                latch.countDown();
                            }
                        }
                    });
                }
                latch.await();
                service.shutdownNow();
                return readRecordCount.get() == topics.length;
            }
        };
        return runnable;
    }
}
