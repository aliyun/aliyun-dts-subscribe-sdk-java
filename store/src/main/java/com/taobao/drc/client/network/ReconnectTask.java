package com.taobao.drc.client.network;

import com.taobao.drc.client.cm.ClusterManagerFacade;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.TransportType;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.EventExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by jianjundeng on 8/17/16.
 */
public class ReconnectTask implements Runnable{

    private static final Log log = LogFactory.getLog(ReconnectTask.class);

    private final EventExecutor executor;
    private final NetworkEndpoint networkEndpoint;
    private final UserConfig userConfig;
    private final ConnectionStateChangeListener stateChangeListener;

    public ReconnectTask(EventExecutor executor, NetworkEndpoint networkEndpoint, UserConfig userConfig,
                         ConnectionStateChangeListener stateChangeListener) {
        this.executor = executor;
        this.networkEndpoint = networkEndpoint;
        this.userConfig = userConfig;
        this.stateChangeListener = stateChangeListener;
    }

    @Override
    public void run() {
        //auto inner retry
        if(!networkEndpoint.isClose()) {
            try {
                log.info("start reconnect,subTopic:" + userConfig.getSubTopic());
                ClusterManagerFacade.askToken(userConfig);
                ClusterManagerFacade.StoreInfo storeInfo = ClusterManagerFacade.fetchStoreInfo(userConfig, userConfig.getTransportType() == TransportType.DRCNET);
                ChannelFuture channelFuture = networkEndpoint.connectToStore(storeInfo, userConfig, stateChangeListener);
                channelFuture.sync();
                log.info("client restart,subTopic:" + userConfig.getSubTopic() + " ,checkpoint:" + userConfig.getCheckpoint().toString());
            }catch (Exception e){
                log.error("reconnect error",e);
                long backOffMs = stateChangeListener.onException(null, e);
                if (backOffMs >= 0) {
                    executor.schedule(new ReconnectTask(executor, networkEndpoint, userConfig, stateChangeListener),
                            backOffMs, TimeUnit.MILLISECONDS);
                    log.error("Reconnect failed, schedule another reconnect after [" + backOffMs + "] milliseconds");
                } else {
                    log.warn("Reconnect failed, do not reconnect anymore", e);
                }
            }
        }
    }
}
