package com.taobao.drc.client.network;

import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.cm.ClusterManagerFacade;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.TransportType;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.EventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by jianjundeng on 8/17/16.
 */
public class ReconnectTask implements Runnable{

    private static final Logger log = LoggerFactory.getLogger(ReconnectTask.class);

    private final EventExecutor executor;
    private final NetworkEndpoint networkEndpoint;
    private final UserConfig userConfig;
    private final ConnectionStateChangeListener stateChangeListener;
    private final CheckpointManager checkpointManager;

    public ReconnectTask(EventExecutor executor, NetworkEndpoint networkEndpoint, UserConfig userConfig,
                         ConnectionStateChangeListener stateChangeListener) {
        this(executor, networkEndpoint, userConfig, stateChangeListener, null);
    }

    public ReconnectTask(EventExecutor executor, NetworkEndpoint networkEndpoint, UserConfig userConfig,
                         ConnectionStateChangeListener stateChangeListener, CheckpointManager checkpointManager) {
        this.executor = executor;
        this.networkEndpoint = networkEndpoint;
        this.userConfig = userConfig;
        this.stateChangeListener = stateChangeListener;
        this.checkpointManager = checkpointManager;
    }

    @Override
    public void run() {
        //auto inner retry
        if(!networkEndpoint.isClose()) {
            try {
                log.info("start reconnect,subTopic:" + userConfig.getSubTopic());
                ClusterManagerFacade.askToken(userConfig);
                ClusterManagerFacade.StoreInfo storeInfo = ClusterManagerFacade.fetchStoreInfo(userConfig, userConfig.getTransportType() == TransportType.DRCNET);
                ChannelFuture channelFuture = networkEndpoint.connectToStore(storeInfo, userConfig, stateChangeListener, checkpointManager);
                channelFuture.sync();
                log.info("client restart,subTopic:" + userConfig.getSubTopic() + " ,checkpoint:" + userConfig.getCheckpoint().toString());
            }catch (Exception e){
                log.error("reconnect error",e);
                long backOffMs = stateChangeListener.onException(null, e);
                if (backOffMs >= 0) {
                    executor.schedule(new ReconnectTask(executor, networkEndpoint, userConfig, stateChangeListener, checkpointManager),
                            backOffMs, TimeUnit.MILLISECONDS);
                    log.error("Reconnect failed, schedule another reconnect after [" + backOffMs + "] milliseconds");
                } else {
                    log.warn("Reconnect failed, do not reconnect anymore", e);
                }
            }
        }
    }
}
