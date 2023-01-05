package com.taobao.drc.client.store.impl;

import com.taobao.drc.client.DataFilterBase;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.impl.RecordsCache;
import com.taobao.drc.client.store.StoreClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractStoreClient implements StoreClient {
    private static final Log log = LogFactory.getLog(AbstractStoreClient.class);
    protected final Listener listener;
    protected final UserConfig userConfig;
    protected final CheckpointManager checkpointManager;
    protected final DataFilterBase filter;
    protected final Map<String, UserConfig> userConfigMap = new ConcurrentHashMap<String, UserConfig>();
    protected final Map<String, CheckpointManager> checkpointManagerMap = new ConcurrentHashMap<String, CheckpointManager>();
    protected final String parseThreadPrefix;
    protected final String notifyThreadPrefix;
    protected DBType dbType = null;
    public AbstractStoreClient(Listener listener, UserConfig userConfig, CheckpointManager checkpointManager, DataFilterBase filter, String parseThreadPrefix, String notifyThreadPrefix) {
        this.listener = listener;
        this.userConfig = userConfig;
        this.checkpointManager = checkpointManager;
        this.filter = filter;
        this.notifyThreadPrefix = notifyThreadPrefix;
        this.parseThreadPrefix = parseThreadPrefix;
    }

    @Override
    public void sendRuntimeLog(String level, String msg) {
        sendRuntimeLog(listener, level, msg);
    }

    public static void sendRuntimeLog(Listener listener, String level, String msg) {
        if (listener != null) {
            try {
                listener.notifyRuntimeLog(level, msg);
            } catch (Exception e) {
                try {
                    listener.handleException(e);
                } catch (Exception e1) {
                    log.error("Listener failed to handle exception [" + e + "]", e1);
                }
            }
        }
    }

    @Override
    public String getMultiSafeTimestamp() {
        String min = null;
        for (CheckpointManager checkpointManager : checkpointManagerMap.values()) {
            if (checkpointManager.getSaveCheckpoint() == null) {
                break;
            }
            if (min == null) {
                min = checkpointManager.getSaveCheckpoint();
            }else{
                Long minLong = Long.valueOf(min);
                Long temp = Long.valueOf(checkpointManager.getSaveCheckpoint());
                if(temp < minLong) {
                    min = String.valueOf(temp);
                }
            }
        }
        return min;
    }

    @Override
    public DBType getRunningDBType() {
        return dbType;
    }

    protected void cloneInner(UserConfig userConfig, UserConfig uc) {
        Checkpoint ck= new Checkpoint(userConfig.getCheckpoint());
        uc.setCheckpoint(ck);

        if(userConfig.getRecordsCache()!=null){
            uc.setRecordsCache(new RecordsCache(userConfig.getRecordsCache()));
        }
        uc.setChannelUrl(userConfig.getChannelUrl());
    }
}
