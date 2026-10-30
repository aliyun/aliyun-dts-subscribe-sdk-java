package com.taobao.drc.client.store.impl;

import com.taobao.drc.client.DataFilterBase;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.impl.RecordsCache;
import com.taobao.drc.client.impl.ListenerContext;
import com.taobao.drc.client.store.StoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractStoreClient implements StoreClient {
    private static final Logger log = LoggerFactory.getLogger(AbstractStoreClient.class);
    protected final Listener listener;
    protected final UserConfig userConfig;
    protected CheckpointManager checkpointManager;
    protected final DataFilterBase filter;
    protected final Map<String, UserConfig> userConfigMap = new ConcurrentHashMap<String, UserConfig>();
    protected final Map<String, CheckpointManager> checkpointManagerMap = new ConcurrentHashMap<String, CheckpointManager>();
    protected final String parseThreadPrefix;
    protected final String notifyThreadPrefix;
    protected DBType dbType = null;
    private final Checkpoint originalCheckpoint;
    private java.util.Set<String> currentTopics;
    private String singleTopic;
    private long retiredConnectionCount;
    private String recoveryReason;
    public AbstractStoreClient(Listener listener, UserConfig userConfig, CheckpointManager checkpointManager, DataFilterBase filter, String parseThreadPrefix, String notifyThreadPrefix) {
        this.listener = listener;
        this.userConfig = userConfig;
        this.checkpointManager = checkpointManager;
        this.filter = filter;
        this.notifyThreadPrefix = notifyThreadPrefix;
        this.parseThreadPrefix = parseThreadPrefix;
        this.originalCheckpoint = userConfig == null || userConfig.getCheckpoint() == null
                ? new Checkpoint() : new Checkpoint(userConfig.getCheckpoint());
        this.recoveryReason = checkpointManager == null ? null : checkpointManager.getStatus().getRecoveryReason();
    }

    /** Build the entire shard set before workers start or aggregate positions are exposed. */
    protected synchronized void prepareTopics(java.util.Collection<String> topics) throws Exception {
        java.util.Set<String> next = new java.util.LinkedHashSet<String>(topics);
        if (next.isEmpty() || next.size() != topics.size()) { throw new IllegalStateException("Empty or duplicate subscription topics"); }
        if (currentTopics != null && !currentTopics.equals(next)) {
            resetForTopology("Subscription topics changed from " + currentTopics + " to " + next);
            for (CheckpointManager manager : checkpointManagerMap.values()) {
                manager.invalidate(); retiredConnectionCount += manager.getConnectionCount();
            }
            checkpointManagerMap.clear(); userConfigMap.clear();
        }
        for (String topic : next) {
            if (!checkpointManagerMap.containsKey(topic)) {
                UserConfig config = (UserConfig) org.apache.commons.beanutils.BeanUtils.cloneBean(userConfig);
                cloneInner(userConfig, config); config.setSubTopic(topic); config.setMultiMode(true);
                CheckpointManager manager = new CheckpointManager(true);
                manager.setMaxPendingAckRanges(config.getMaxPendingAckRanges());
                manager.setRecoveryReason(recoveryReason);
                userConfigMap.put(topic, config); checkpointManagerMap.put(topic, manager);
            }
        }
        currentTopics = next;
    }

    protected synchronized void prepareSingleTopic(String topic) {
        if (singleTopic != null && !singleTopic.equals(topic)) {
            resetForTopology("Subscription topic changed from " + singleTopic + " to " + topic);
            checkpointManager.invalidate(); retiredConnectionCount += checkpointManager.getConnectionCount();
            checkpointManager = new CheckpointManager(false);
            checkpointManager.setMaxPendingAckRanges(userConfig.getMaxPendingAckRanges());
            checkpointManager.setRecoveryReason(recoveryReason);
        }
        singleTopic = topic;
        userConfig.setSubTopic(topic);
    }

    private void resetForTopology(String reason) {
        String timestamp = originalCheckpoint.getTimestamp();
        if (!org.apache.commons.lang3.StringUtils.isNumeric(timestamp)) {
            throw new com.taobao.drc.client.checkpoint.UnsafeRecoveryException(reason
                    + "; original start has no portable timestamp. Supply a verified timestamp with a new client.");
        }
        recoveryReason = reason + "; shard mapping is unverified. Replaying from original timestamp " + timestamp;
        log.warn(recoveryReason);
        userConfig.setCheckpoint(new Checkpoint(null, null, null, timestamp));
    }

    protected java.util.concurrent.ExecutorService createMultiServiceExecutor(int size, String threadPrefix) {
        return java.util.concurrent.Executors.newFixedThreadPool(size,
                new com.taobao.drc.client.network.DRCClientThreadFactory(threadPrefix));
    }

    @Override public synchronized long getConnectionCount() {
        long result = retiredConnectionCount + checkpointManager.getConnectionCount();
        for (CheckpointManager manager : checkpointManagerMap.values()) { result += manager.getConnectionCount(); }
        return result;
    }

    @Override
    public void sendRuntimeLog(String level, String msg) {
        sendRuntimeLog(listener, level, msg);
    }

    public static void sendRuntimeLog(Listener listener, String level, String msg) {
        if (listener != null) {
            ListenerContext.enter();
            try {
                listener.notifyRuntimeLog(level, msg);
            } catch (Exception e) {
                try {
                    listener.handleException(e);
                } catch (Exception e1) {
                    log.error("Listener failed to handle exception [" + e + "]", e1);
                }
            } finally {
                ListenerContext.exit();
            }
        }
    }

    @Override
    public synchronized String getMultiSafeTimestamp() {
        String min = null;
        for (CheckpointManager checkpointManager : checkpointManagerMap.values()) {
            if (checkpointManager.getSaveCheckpoint() == null) {
                return null;
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
    public synchronized Map<String, CheckpointManager.Status> getCheckpointStatus() {
        Map<String, CheckpointManager.Status> result = new java.util.HashMap<String, CheckpointManager.Status>();
        if (checkpointManagerMap.isEmpty()) {
            if (userConfig.getSubTopic() != null) {
                result.put(userConfig.getSubTopic(), checkpointManager.getStatus());
            }
        } else {
            for (Map.Entry<String, CheckpointManager> entry : checkpointManagerMap.entrySet()) {
                result.put(entry.getKey(), entry.getValue().getStatus());
            }
        }
        return java.util.Collections.unmodifiableMap(result);
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
