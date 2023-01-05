package com.taobao.drc.client.network;

import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.network.dstore.DStoreConsumer;
import com.taobao.drc.client.network.dstore.SubscriptionConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DStore network endpoint
 * @author xusheng.zkw
 */
public class DStoreNetworkEndpoint {
    private static final Log log = LogFactory.getLog(DStoreNetworkEndpoint.class);
    private final Map<String, DStoreConsumer> channels = new ConcurrentHashMap<String, DStoreConsumer>();
    private Listener listener;

    public void setMessageListener(Listener listener) {
        this.listener = listener;
    }



    /**
     * this method will blocking caller thread.
     * @param userConfig
     */
    public void connectToStoreThenWait(UserConfig userConfig, CheckpointManager checkpointManager) {
        if (!StringUtils.isNumeric(userConfig.getPollTimeoutMs())) {
            userConfig.setPollTimeoutMs("200");
        }
        /*
         * make sure the old consumer is closed
         */
        try {
            DStoreConsumer consumer = channels.get(userConfig.getSubTopic());
            if (null != consumer) {
                consumer.interrupt();
                channels.remove(userConfig.getSubTopic());
            }
        } catch (Throwable e) {
        }
        DStoreConsumer consumer = new SubscriptionConsumer(listener);
        consumer.init(userConfig, checkpointManager);
        channels.put(userConfig.getSubTopic(), consumer);
        consumer.start();
        consumer.sync();
    }

    public void close() {
        try {
            for (DStoreConsumer consumer : channels.values()) {
                log.info("close channel [" + consumer.getConfig().getSubTopic() + "]");
                consumer.interrupt();
            }
            channels.clear();
        } catch (Throwable e) {
            log.warn("DStore network endpoint close with error:" + e.getMessage());
        }
    }
}
