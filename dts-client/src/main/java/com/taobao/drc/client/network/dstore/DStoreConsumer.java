package com.taobao.drc.client.network.dstore;

import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;

/***
 * dstore kafka consumer
 * @author xusheng.zkw
 */
public interface DStoreConsumer {
    void init(UserConfig userConfig, CheckpointManager checkpointManager);
    void close();
    UserConfig getConfig();
    void start();
    void run();
    void sync();
    void interrupt();
}
