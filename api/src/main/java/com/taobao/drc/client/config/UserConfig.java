package com.taobao.drc.client.config;

import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.DataFilterBase;
import com.taobao.drc.client.impl.RecordsCache;
import com.taobao.drc.client.SubscribeChannel;
import com.taobao.drc.client.enums.MessageType;
import com.taobao.drc.client.enums.TransportType;
import com.taobao.drc.client.impl.LocalityFile;
import com.taobao.drc.client.sql.SqlEngine;
import com.taobao.drc.client.utils.Constant;
import org.apache.commons.lang3.StringUtils;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jianjundeng on 3/3/15.
 */
public class UserConfig {

    private Long id;

    private String clusterUrl;

    private volatile Checkpoint checkpoint;

    private String token;

    private String userName;

    private String password;

    private String db;

    private String subTopic;

    private DataFilterBase dataFilter;

    private boolean needUKRecord;

    private String blackList;

    private boolean askSelfUnit;

    private boolean requireTx = true;

    private String filterInfo="drc.t*x_begin4unit_mark_[0-9]*|*.drc_txn";

    private int retryTimes = 100;

    private MessageType messageType = MessageType.TEXT;

    private TransportType transportType=TransportType.HTTP;

    private volatile String store;

    private boolean usePublicIp;

    private String regionNoBlackList;

    private boolean caseSensitive;

    private boolean crc32Check;

    private boolean trimToLong;

    private long persistPeriod=500l;

    private LocalityFile localityFile;

    private RecordsCache recordsCache;

    private volatile String txBeginTimestamp;

    private int hashMask;

    private String hashKey;

    private volatile String saveCheckpoint;

    private String regionInfo;

    private long maxPendingRecords = 1024L;

    private int readTimeoutSeconds = 120;

    private String channelUrl;
    private String regionCode;
    private SqlEngine engine;
    private SubscribeChannel channel;
    private String reversed;
    private String clientIp;
    private boolean skipAuth = true;
    private String fetchWaitMs;
    private String pollTimeoutMs;
    private String saslMechanism;
    private String securityProtocal;
    private String pollBytesSize;
    private String pollRecordsSize;
    private boolean askOtherUnitUseThreadID = false;
    private boolean askSelfUnitUseThreadID = false;
    private String dtsMark;
    private transient String dtsChannelId;
    private boolean filterEmptyTxnMark = true;
    private int socketTimeOut = 1000 * 60;

    private boolean fnMatchFilter = true;

    private boolean multiServiceRetry = true;

    private int dataFlowRpsLimit = -1;

    private String writerType;

    public UserConfig(){
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    private boolean needEncodingForDRCNet(String keyName) {
        if (StringUtils.equalsIgnoreCase(keyName, "password") ||
                StringUtils.equalsIgnoreCase(keyName, "username") ||
                StringUtils.equalsIgnoreCase(keyName, "group")) {
            return true;
        } else {
            return false;
        }
    }


    public String toUrlString() throws Exception {
        Map<String, String> params = getParams();
        List<String> list = new ArrayList<String>();
        // For drc net, string is split by '&' and then split by '=' to divide key and value, user name , password and group may
        // contain special character like '&,=' which will lead to the wrong interpretation for the string.
        // So we convert the filed may contains special character use URLEncoder
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String value = entry.getValue();
            String key = entry.getKey();
            if (needEncodingForDRCNet(key)) {
                value = URLEncoder.encode(value, "UTF-8");
            }
            list.add(key + "=" + value);
        }
        return StringUtils.join(list, "&");
    }

    public Map<String, String> getParams() throws Exception {
        Map<String, String> params = new HashMap<String, String>();
        if (checkpoint.getTimestamp() != null) {
            params.put("checkpoint", checkpoint.getTimestamp());
        } else if (checkpoint.getPosition() != null) {
            params.put("checkpoint", checkpoint.getPosition());
            params.put("serverid", checkpoint.getServerId());
        } else {
            throw new Exception("Wrong checkpoint: " + checkpoint.toString());
        }

        params.put("token", token);
        params.put("group", userName);
        params.put("username", userName);
        params.put("password", password);
        params.put("subGroup", subTopic);
        params.put("filter.conditions", dataFilter.getConnectStoreFilterConditions());
        params.put("topic",subTopic);
        if (askSelfUnit) {
            params.put("filter.drcmark",filterInfo);
            if (filterInfo.startsWith("!")) {
                params.put("filter.alithreadid", "off");
            } else {
                params.put("filter.alithreadid", "on");
            }
        }
        if(needUKRecord){
            params.put("client.needukrecord","true");
        }

        if(transportType != null && transportType.equals(TransportType.DRCNET)) {
            params.put("useDrcNet", "true");

            if (messageType.equals(MessageType.BINARY)) {
                params.put("writer.type", "data");
            }
        } else {
            if (messageType.equals(MessageType.BINARY)) {
                params.put("writer.type", "data");
            }
        }

        //for ob store, if client set
        if (writerType != null) {
            params.put("writer.type", writerType);
        }

        if (blackList != null) {
            params.put("filter.blacklist", blackList);
        }
        if (!requireTx) {
            params.put("filter.txn", "false");
        }
        if(regionNoBlackList!=null){
            params.put("slave.regionno",regionNoBlackList);
        }
        if(caseSensitive){
            params.put("casesensitive","true");
        }
        if(trimToLong){
            params.put("filter.longtype", "true");
        }
        if(hashMask>0){
            params.put("hashMask", String.valueOf(hashMask));
            params.put("hashKey", hashKey);
        }

        params.put("client.version", Constant.DTS_CLIENT_VERSION);
        return params;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public String getSubTopic() {
        return subTopic;
    }

    public void setSubTopic(String subTopic) {
        this.subTopic = subTopic;
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    public void setClusterUrl(String clusterUrl) {
        this.clusterUrl = clusterUrl;
        //set default channel cluster url
        setChannelUrl(clusterUrl);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public boolean isAskSelfUnit() {
        return askSelfUnit;
    }

    public String getBlackList() {
        return blackList;
    }

    public void setBlackList(String blackList) {
        this.blackList = blackList;
    }

    public boolean isRequireTx() {
        return requireTx;
    }

    public void setRequireTx(boolean requireTx) {
        this.requireTx = requireTx;
    }

    public Checkpoint getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(Checkpoint checkpoint) {
        this.checkpoint = checkpoint;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public String getStore() {
        return store;
    }

    public void setStore(String store) {
        this.store = store;
    }

    public String getFilterInfo() {
        return filterInfo;
    }

    public void setFilterInfo(String filterInfo) {
        this.filterInfo = filterInfo;
    }

    public void setNeedUKRecord(boolean needUKRecord) {
        this.needUKRecord = needUKRecord;
    }

    public boolean isUsePublicIp() {
        return usePublicIp;
    }

    public void setUsePublicIp(boolean usePublicIp) {
        this.usePublicIp = usePublicIp;
    }

    public String getRegionNoBlackList() {
        return regionNoBlackList;
    }

    public void setRegionNoBlackList(String regionNoBlackList) {
        this.regionNoBlackList = regionNoBlackList;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public boolean isCrc32Check() {
        return crc32Check;
    }

    public void setCrc32Check(boolean crc32Check) {
        this.crc32Check = crc32Check;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public void setTrimToLong(boolean trimToLong) {
        this.trimToLong = trimToLong;
    }

    public void setDataFilter(DataFilterBase dataFilter) {
        this.dataFilter = dataFilter;
    }

    public DataFilterBase getDataFilter(){
        return dataFilter;
    }

    public long getPersistPeriod() {
        return persistPeriod;
    }

    public void setPersistPeriod(long persistPeriod) {
        this.persistPeriod = persistPeriod;
    }

    public LocalityFile getLocalityFile() {
        return localityFile;
    }

    public void setLocalityFile(LocalityFile localityFile) {
        this.localityFile = localityFile;
    }

    public RecordsCache getRecordsCache() {
        return recordsCache;
    }

    public void setRecordsCache(RecordsCache recordsCache) {
        this.recordsCache = recordsCache;
    }

    public String getTxBeginTimestamp() {
        return txBeginTimestamp;
    }

    public void setTxBeginTimestamp(String txBeginTimestamp) {
        this.txBeginTimestamp = txBeginTimestamp;
    }

    public void setHashMask(int hashMask) {
        this.hashMask = hashMask;
    }

    public void setHashKey(String hashKey) {
        this.hashKey = hashKey;
    }

    public int getHashMask(){
        return hashMask;
    }

    public String getHashKey(){
        return hashKey;
    }

    public TransportType getTransportType() {
        return transportType;
    }

    public void setTransportType(TransportType transportType) {
        this.transportType = transportType;
    }

    public String getSaveCheckpoint() {
        return saveCheckpoint;
    }

    public void setSaveCheckpoint(String saveCheckpoint) {
        this.saveCheckpoint = saveCheckpoint;
    }

    public String getRegionInfo() {
        return regionInfo;
    }

    public void setRegionInfo(String regionInfo) {
        this.regionInfo = regionInfo;
    }

    public void setAskSelfUnit(boolean askSelfUnit) {
        this.askSelfUnit = askSelfUnit;
    }

    public long getMaxPendingRecords() {
        return maxPendingRecords;
    }

    public void setMaxPendingRecords(long maxPendingRecords) {
        this.maxPendingRecords = maxPendingRecords;
    }

    public int getReadTimeoutSeconds() {
        return readTimeoutSeconds;
    }

    public void setReadTimeoutSeconds(int readTimeoutSeconds) {
        this.readTimeoutSeconds = readTimeoutSeconds;
    }

    public void setChannelUrl(String channelUrl) {
        this.channelUrl = channelUrl;
    }

    public String getChannelUrl() {
        return channelUrl;
    }

    public String getRegionCode() {
        return regionCode;
    }

    public void setSqlEngine(SqlEngine engine) {
        this.engine = engine;
    }

    public SqlEngine getEngine() {
        return engine;
    }

    public SubscribeChannel getSubscribeChannel() {
        return channel;
    }

    public void setSubscribeChannel(SubscribeChannel channel) {
        this.channel = channel;
    }

    public void setRegionCode(String regionCode) {
        this.regionCode = regionCode;
    }

    public String getReversed() {
        return reversed;
    }

    public void setReversed(String reversed) {
        this.reversed = reversed;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public boolean isSkipAuth() {
        return skipAuth;
    }

    public void setSkipAuth(boolean skipAuth) {
        this.skipAuth = skipAuth;
    }

    public void setFetchWaitMs(String fetchWaitMs) {
        this.fetchWaitMs = fetchWaitMs;
    }

    public void setPollTimeoutMs(String pollTimeoutMs) {
        this.pollTimeoutMs = pollTimeoutMs;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public void setSecurityProtocal(String securityProtocal) {
        this.securityProtocal = securityProtocal;
    }

    public void setPollBytesSize(String pollBytesSize) {
        this.pollBytesSize = pollBytesSize;
    }

    public SubscribeChannel getChannel() {
        return channel;
    }

    public String getFetchWaitMs() {
        return fetchWaitMs;
    }

    public String getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public String getSecurityProtocal() {
        return securityProtocal;
    }

    public void askSelfUnitUseThreadID() {
        this.askSelfUnitUseThreadID = true;
    }

    public void askOtherUnitUseThreadID() {
        this.askOtherUnitUseThreadID = true;
    }

    public boolean isAskOtherUnitUseThreadID() {
        return askOtherUnitUseThreadID;
    }

    public boolean isAskSelfUnitUseThreadID() {
        return askSelfUnitUseThreadID;
    }

    public String getDtsChannelId() {
        return dtsChannelId;
    }

    public void setDtsChannelId(String dtsChannelId) {
        this.dtsChannelId = dtsChannelId;
    }

    public String getDtsMark() {
        return dtsMark;
    }

    public void setDtsMark(String dtsMark) {
        this.dtsMark = dtsMark;
    }

    public boolean isFilterEmptyTxnMark() {
        return filterEmptyTxnMark;
    }

    public void setFilterEmptyTxnMark(boolean filterEmptyTxnMark) {
        this.filterEmptyTxnMark = filterEmptyTxnMark;
    }

    public String getPollBytesSize() {
        return pollBytesSize;
    }

    public String getPollRecordsSize() {
        return pollRecordsSize;
    }

    public void setPollRecordsSize(String pollRecordsSize) {
        this.pollRecordsSize = pollRecordsSize;
    }

    public void setSocketTimeOut(int socketTimeOut) {
        this.socketTimeOut = socketTimeOut;
    }

    public int getSocketTimeOut() {
        return socketTimeOut <= 0 ? Integer.MAX_VALUE : socketTimeOut;
    }

    public boolean isFnMatchFilter() {
        return fnMatchFilter;
    }

    public void setFnMatchFilter(boolean fnMatchFilter) {
        this.fnMatchFilter = fnMatchFilter;
    }

    public boolean isMultiServiceRetry() {
        return multiServiceRetry;
    }

    public void setMultiServiceRetry(boolean multiServiceRetry) {
        this.multiServiceRetry = multiServiceRetry;
    }

    public int getDataFlowRpsLimit() {
        return dataFlowRpsLimit;
    }

    public void setDataFlowRpsLimit(int dataFlowRpsLimit) {
        this.dataFlowRpsLimit = dataFlowRpsLimit;
    }

    public void setWriterType(String writerType) {
        this.writerType = writerType;
    }

    public String getWriterType() {
        return this.writerType;
    }
}
