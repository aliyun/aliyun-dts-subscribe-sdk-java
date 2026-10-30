package com.taobao.drc.client.store;

import com.taobao.drc.client.enums.DBType;
import io.netty.util.concurrent.Future;

import java.util.concurrent.Callable;

public interface StoreClient {
    /** Monotonic count of established connections within this StoreClient. */
    default long getConnectionCount() { return 0; }
    default java.util.Map<String, com.taobao.drc.client.checkpoint.CheckpointManager.Status> getCheckpointStatus() {
        return java.util.Collections.emptyMap();
    }
    /**
     * DStore subscribe client will do nothing.
     * @param userCtlBytes
     * @return
     */
    @Deprecated
    Future writeUserCtlMessage(byte[] userCtlBytes);
    void stopService();
    Callable<Boolean> startService() throws Exception;
    Callable<Boolean> startMultiService() throws Exception;
    String getMultiSafeTimestamp();
    void sendRuntimeLog(String level, String msg);
    DBType getRunningDBType();
}
