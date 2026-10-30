package com.taobao.drc.client.impl;

import com.taobao.drc.client.DRCClient;
import com.taobao.drc.client.DataFilter;
import com.taobao.drc.client.DataFilterBase;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.SubscribeChannel;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.cm.ClusterManagerFacade;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.enums.MessageType;
import com.taobao.drc.client.enums.Status;
import com.taobao.drc.client.enums.TransportType;
import com.taobao.drc.client.network.dstore.DStoreOffsetNotExistException;
import com.taobao.drc.client.sql.SqlEngine;
import com.taobao.drc.client.store.StoreClient;
import com.taobao.drc.client.store.impl.DRCStoreClientImpl;
import com.taobao.drc.client.store.impl.DStoreClientImpl;
import com.taobao.drc.client.utils.Constant;
import io.netty.util.concurrent.Future;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;

import static com.taobao.drc.client.enums.Status.INIT;
import static com.taobao.drc.client.enums.Status.RAW;
import static com.taobao.drc.client.utils.CommonUtils.isValidTopicName;
import static com.taobao.drc.client.utils.Constant.*;

/**
 * Implement the DRCClient, but it's only a proxy.
 * The main job is accepting user input and being agency to DStoreClientImpl/DRCStoreClientImpl.
 */
public class DRCClientImpl implements DRCClient {

    private static final Logger log = LoggerFactory.getLogger(DRCClientImpl.class);

    private DataFilterBase filter;

    private volatile Status status = RAW;

    private boolean autoRetry = true;

    private volatile boolean quit = false;

    private UserConfig userConfig = new UserConfig();

    private Properties properties = new Properties();

    private Listener listener;

    private String parseThreadPrefix;

    private String notifyThreadPrefix;

    private volatile StoreClient client;
    private final Object lifecycleLock = new Object();
    private volatile Thread worker;
    private volatile Throwable lastFailure;
    private Boolean multiSubscription;
    private Checkpoint initialCheckpoint;
    private SubscribeChannel clientChannel;

    public DRCClientImpl(String propertiesFilename)
            throws IOException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(propertiesFilename);
        properties.load(inputStream);
    }

    public DRCClientImpl(Reader reader)
            throws IOException {
        properties.load(reader);
    }

    public DRCClientImpl(Properties properties) {
        this.properties = properties;
    }

    @Override
    @Deprecated
    public void addDRCConfigure(final String name, final String value) {
    }


    @Override
    @Deprecated
    public Map<String, String> getDRCConfigures() {
        return new HashMap<String, String>();
    }

    @Override
    @Deprecated
    public String getDRCConfigure(final String name) {
        return null;
    }

    @Override
    @Deprecated
    public void addUserParameter(final String name, final String value) {

    }

    @Override
    @Deprecated
    public Map<String, String> getUserParameters() {
        return new HashMap<String, String>();
    }

    @Override
    @Deprecated
    public String getUserParameter(final String name) {
        return null;
    }

    /**
     * Used by users to initialize the service by gmtModified.
     *
     * @param groupName      is the name of the users' group.
     * @param dbName         is the partial database name.
     * @param identification is the user's password.
     * @param startingPoint  is either "checkpoint" or "gmt_modified".
     * @param localFilename  path to binary log file.
     * @throws IOException log file related IO errors.
     */
    @Override
    public void initService(final String groupName,
                            final String dbName,
                            final String identification,
                            final String startingPoint,
                            final String localFilename)
            throws Exception {
        initService(groupName, dbName, identification, startingPoint, PRIM_META, PRIM_TKNM, PRIM_INST, localFilename);
    }

    /**
     * Initialize the service by manually setting restarted checkpoint.
     * Notice that the user should also provide the respective metadata version
     * and assigned task name.
     *
     * @param groupName      is the name of the users' group.
     * @param dbName         is the partial database name.
     * @param identification is the user's password.
     * @param startingPoint  is either "checkpoint" or "gmt_modified".
     * @param metaVersion    is the metadata version.
     * @param taskName       is the task name.
     * @param instance       is the address of the last database instance.
     * @param localFilename  is path to binary log file.
     * @throws IOException if the log file cannot be created.
     */
    @Override
    public void initService(final String groupName,
                            final String dbName,
                            final String identification,
                            final String startingPoint,
                            final String metaVersion,
                            final String taskName,
                            final String instance,
                            final String localFilename) throws Exception {


        if (status != RAW) {
            throw new Exception("server state is error,current state:" + status);
        }
        Checkpoint checkpoint = new Checkpoint();
        buildCheckpoint(checkpoint, startingPoint, instance);
        userConfig.setCheckpoint(checkpoint);
        initialCheckpoint = new Checkpoint(checkpoint);
        initConfig(groupName, dbName, identification, localFilename);
        log.info("initialize the service with starting point: " + startingPoint);
        status=INIT;
    }

    /**
     * Used by users to reconnect according to the log fle.
     *
     * @param groupName      is the name of the users' group.
     * @param dbName         is the partial database name.
     * @param identification is the user's password.
     * @param localFilename  is path to binary log file.
     * @throws Exception if the log file not exists.if the log file is not readable.
     */
    @Override
    public void initService(final String groupName,
                            final String dbName,
                            final String identification,
                            final String localFilename)
            throws Exception {

        Checkpoint location = getLocalInfo(localFilename);
        initService(groupName, dbName, identification,
                location.getPosition().equals("@") ? location.getTimestamp() : location.getPosition(),
                PRIM_META, PRIM_TKNM, location.getServerId(), localFilename);
    }

    @Override
    public void initService(final String groupName,
                            final String dbName,
                            final String identification,
                            final Checkpoint checkpoint,
                            final String localFilename) throws Exception {
        if (status != RAW) {
            throw new Exception("server state is error,current state:" + status);
        }
        userConfig.setCheckpoint(new Checkpoint(checkpoint));
        initialCheckpoint = new Checkpoint(checkpoint);
        initConfig(groupName, dbName, identification, localFilename);
        log.info("Initialize the service with starting point: " + checkpoint.toString());
        status=INIT;
    }

    /**
     * init param
     *
     * @param groupName
     * @param dbName
     * @param identification
     * @param localFilename
     * @throws Exception
     */
    private void initConfig(String groupName, String dbName, String identification, String localFilename) throws Exception {
        userConfig.setClusterUrl(properties.getProperty(CM_URL));
        userConfig.setDb(dbName);
        userConfig.setUserName(groupName);
        userConfig.setPassword(identification);
        if (dbName != null) {
            if (!isValidTopicName(dbName)) {
                log.debug("Not a valid dbName: " + dbName);
                // db is a branched db name
                filter.setBranchDb(dbName);
            }
        }

        userConfig.setDataFilter(filter);

        if (properties.getProperty(USE_DRCNET) != null && Boolean.valueOf(properties.getProperty(USE_DRCNET)) == true) {
            userConfig.setTransportType(TransportType.DRCNET);
        }
        if (StringUtils.equals(properties.getProperty(SERVER_MESSAGE_TYPE), BINARY_TYPE)) {
            userConfig.setMessageType(MessageType.BINARY);
        } else {
            userConfig.setMessageType(MessageType.TEXT);
        }

        //checkpoint persistence period
        if (localFilename != null) {
            LocalityFile binlogFile = new LocalityFile(localFilename, 0, 104857600);
            userConfig.setLocalityFile(binlogFile);
        }
        if (properties.getProperty(CHECKPOINT_PERIOD) != null) {
            long period = Long.valueOf(properties.getProperty(CHECKPOINT_PERIOD));
            userConfig.setPersistPeriod(period);
        }
        //record cache
        if (properties.getProperty(NEED_RECORD_CACHE) != null && Boolean.valueOf(properties.getProperty(NEED_RECORD_CACHE)) == true) {
            RecordsCache recordsCache = new RecordsCache();
            userConfig.setRecordsCache(recordsCache);
            if (properties.getProperty(MAX_NUM_RECORDS_PER_MESSAGE) != null) {
                recordsCache.setmaxRecordsBatched(Integer.parseInt(properties.getProperty(MAX_NUM_RECORDS_PER_MESSAGE)));
            }
            if (properties.getProperty(MAX_TIMEOUT_PER_MESSAGE) != null) {
                recordsCache.setmaxTimeoutBatched(Integer.parseInt(properties.getProperty(MAX_TIMEOUT_PER_MESSAGE)));
            }
            if (properties.getProperty(MAX_NUM_TX_PER_MESSAGE) != null) {
                recordsCache.setMaxTxnsBatched(Integer.parseInt(properties.getProperty(MAX_NUM_TX_PER_MESSAGE)));
            }
            if (properties.getProperty(MAX_NUM_RECORDS_CACHED) != null) {
                recordsCache.setMaxRecordsCached(Integer.parseInt(properties.getProperty(MAX_NUM_RECORDS_CACHED)));
            }
            if (properties.getProperty(MAX_BYTES_RECORDS_CACHED) != null) {
                recordsCache.setMaxBytesCached(Integer.parseInt(properties.getProperty(MAX_BYTES_RECORDS_CACHED)));
            }
        }
        //region No black list(thread id,cluster id)
        if (properties.getProperty(REGION_NO_BLACK_LIST) != null) {
            String regionNoBlackList = properties.getProperty(REGION_NO_BLACK_LIST);
            userConfig.setRegionNoBlackList(regionNoBlackList);
        }
        //get hash info
        if (properties.getProperty(CLIENT_HASH_MASK) != null) {
            int hashSum = Integer.parseInt(properties.getProperty(CLIENT_HASH_MASK));
            String hashIndex = properties.getProperty(CLIENT_HASH_KEY);
            userConfig.setHashMask(hashSum);
            userConfig.setHashKey(hashIndex);
        }
        if (properties.getProperty(CLIENT_MAX_PENDING_RECORDS) != null) {
            userConfig.setMaxPendingRecords(Integer.parseInt(properties.getProperty(CLIENT_MAX_PENDING_RECORDS)));
        }
        if (properties.getProperty(CLIENT_READ_TIMEOUT_SECONDS) != null) {
            userConfig.setReadTimeoutSeconds(Integer.parseInt(properties.getProperty(CLIENT_READ_TIMEOUT_SECONDS)));
        }
        if (properties.getProperty(SERVER_MAX_RETRIED_TIMES) != null) {
            userConfig.setRetryTimes(Integer.parseInt(properties.getProperty(SERVER_MAX_RETRIED_TIMES)));
        }

        if (properties.getProperty(REGION_CODE) != null) {
            userConfig.setRegionCode(properties.getProperty(REGION_CODE));
        }

        if (properties.getProperty(REVERSED) != null) {
            userConfig.setReversed(properties.getProperty(REVERSED));
        }

        if (properties.getProperty(CLIENT_IP) != null) {
            userConfig.setClientIp(properties.getProperty(CLIENT_IP));
        }

        if (properties.getProperty(SKIP_AUTH) != null) {
            userConfig.setSkipAuth(Boolean.valueOf(properties.getProperty(SKIP_AUTH)));
        }

        if (properties.getProperty(FETCH_WAIT_MS) != null) {
            userConfig.setFetchWaitMs(properties.getProperty(FETCH_WAIT_MS));
        }

        if (properties.getProperty(POLL_TIMEOUT_MS) != null) {
            userConfig.setPollTimeoutMs(properties.getProperty(POLL_TIMEOUT_MS));
        }

        if (properties.getProperty(SASL_MECHANISM) != null) {
            userConfig.setSaslMechanism(properties.getProperty(SASL_MECHANISM));
        }

        if (properties.getProperty(SECURITY_PROTOCOL) != null) {
            userConfig.setSecurityProtocal(properties.getProperty(SECURITY_PROTOCOL));
        }

        if (properties.getProperty(POLL_BYTES_SIZE) != null) {
            userConfig.setPollBytesSize(properties.getProperty(POLL_BYTES_SIZE));
        }

        if (properties.getProperty(POLL_RECORDS_SIZE) != null) {
            userConfig.setPollRecordsSize(properties.getProperty(POLL_RECORDS_SIZE));
        }
        userConfig.setMaxPendingAckRanges(Integer.parseInt(properties.getProperty(MAX_PENDING_ACK_RANGES, "0")));

        String kafkaSocketTimeOut = properties.getProperty(SOCKET_TIMEOUT_MS);
        if (StringUtils.isNotBlank(kafkaSocketTimeOut) && StringUtils.isNumeric(kafkaSocketTimeOut)) {
            userConfig.setSocketTimeOut(Integer.parseInt(properties.getProperty(SOCKET_TIMEOUT_MS)));
        }

        userConfig.setFnMatchFilter(properties.getProperty(FILTER_FNMATCH) == null || Boolean.valueOf(properties.getProperty(FILTER_FNMATCH)) == true);

        //multiService auto retry
        userConfig.setMultiServiceRetry(Boolean.valueOf(properties.getProperty(CLIENT_MULTISERVICE_RETRY, Boolean.TRUE.toString())));

        //??
        String dataFlowRps = properties.getProperty(DATA_FLOW_RPS_LIMIT);
        if (StringUtils.isNotBlank(dataFlowRps) && StringUtils.isNumeric(dataFlowRps)) {
            userConfig.setDataFlowRpsLimit(Integer.parseInt(dataFlowRps));
        }

        if (properties.getProperty(WRITER_TYPE) != null) {
            userConfig.setWriterType(properties.getProperty(WRITER_TYPE));
        }
    }

    private void buildCheckpoint(Checkpoint checkpoint, String startingPoint, String instance) {
        if (StringUtils.isEmpty(startingPoint)) {
            throw new IllegalArgumentException
                    ("Error startingPoint is empty.");
        } else if (startingPoint.contains("@")) {
                checkpoint.setPosition(startingPoint);
        } else {
            if (startingPoint.length() == 13) {
                throw new IllegalArgumentException
                        ("Error the unit of the starting time is second, but " +
                                startingPoint + " is in ms");
            }
            checkpoint.setTimestamp(startingPoint);
        }
        checkpoint.setServerId(instance);
    }

    @Override
    public Thread startService() throws Exception {
        return start(false);
    }

    @Override
    public Thread startMultiService() throws Exception {
        return start(true);
    }

    private Thread start(boolean multi) {
        synchronized (lifecycleLock) {
            if (status != INIT || (worker != null && worker.isAlive())) {
                throw new IllegalStateException("Cannot start subscription in state " + status + "; join then resetService first");
            }
            if (multiSubscription != null && multiSubscription != multi) {
                throw new IllegalStateException("Changing startService/startMultiService requires a new client");
            }
            multiSubscription = multi;
            userConfig.setMultiMode(multi);
            quit = false; lastFailure = null; status = Status.START;
            worker = startWithFailover(multi);
            worker.setName("DTSSubscriptionCoordinator");
            worker.start();
            log.info("Started subscription, client version " + Constant.DTS_CLIENT_VERSION);
            return worker;
        }
    }

    private Thread startWithFailover(final boolean isMulti) {
        return new Thread() {
            @Override
            public void run() {
                final int maxRetryTimes = isMulti && userConfig.isMultiServiceRetry() ? Integer.MAX_VALUE : userConfig.getRetryTimes();
                int retriedTimes = 0;
                try {
                do {
                    if (quit) { break; }
                    Throwable lastException = null;
                    StoreClient selected = null;
                    long connectionsBeforeAttempt = 0;
                    try {
                        selected = switchClient(isMulti);
                        if (selected != null) { connectionsBeforeAttempt = selected.getConnectionCount(); }
                        Callable<Boolean> attempt = null;
                        synchronized (lifecycleLock) {
                            if (quit || selected == null) { break; }
                            client = selected;
                            // DTS only creates a Callable here. Capture its
                            // cancellation generation atomically with stop.
                            if (selected instanceof DStoreClientImpl) {
                                attempt = isMulti ? selected.startMultiService() : selected.startService();
                            }
                        }
                        if (attempt == null) {
                            // DRC preparation can query CM: keep it outside the
                            // lock and clean up if stop raced with preparation.
                            try {
                                attempt = isMulti ? selected.startMultiService() : selected.startService();
                            } finally {
                                if (quit) {
                                    try { selected.stopService(); }
                                    catch (Throwable cleanupError) {
                                        log.warn("Failed to close stopped store after preparation", cleanupError);
                                    }
                                }
                            }
                            if (quit) { break; }
                        }
                        boolean callResult = attempt.call();
                        if (callResult) {
                            retriedTimes = 0;
                        }
                    } catch (DStoreOffsetNotExistException e) {
                        lastFailure = e;
                        log.error("Recovery data is unavailable; subscription stopped", e);
                        break;
                    } catch (Throwable e) {
                        lastException = e;
                        if (!quit) { lastFailure = e; }
                        log.warn("retry times [" + maxRetryTimes + "], retried [" + retriedTimes + "], errors:", e);
                        if (selected != null) {
                            selected.sendRuntimeLog("WARN","retry times [" + maxRetryTimes + "], retried [" + retriedTimes + "], errors:" + e.getMessage());
                        }
                    }
                    if(quit){
                        break;
                    }
                    if (selected != null && selected.getConnectionCount() > connectionsBeforeAttempt) { retriedTimes = 0; }
                    if (autoRetry) {
                        if ((maxRetryTimes != -1 && retriedTimes >= maxRetryTimes)) {
                            log.error("Reached max retry times [" + maxRetryTimes + "], retried [" + retriedTimes + "]", lastException);
                            throw new RuntimeException("Client Retry exceed max retry time", lastException);
                        }

                        if (ifExceptionCannotRetry(lastException)) {
                            log.error("Client this error cannot be retried: ", lastException);
                            throw new RuntimeException("Client this error cannot be retried: ", lastException);
                        }

                        log.info("client retry: max retries [" + maxRetryTimes + "], retried [" + retriedTimes + "] times, sleep 10s");
                        try {
                            TimeUnit.SECONDS.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        if (quit) { break; }
                        log.info("start retry");
                        if (selected != null) {
                            selected.sendRuntimeLog("WARN", "After broken, retry cluster manager " + retriedTimes + " out of " + maxRetryTimes);
                        }
                        retriedTimes++;
                    } else {
                        log.warn("AutoRetry is disabled");
                        break;
                    }
                } while (!quit);
                log.info("client is quit");
                } catch (RuntimeException | Error terminal) {
                    if (lastFailure == null) { lastFailure = terminal; }
                    throw terminal;
                } finally {
                    StoreClient closing = client;
                    if (closing != null) {
                        try { closing.stopService(); }
                        catch (Throwable e) { if (lastFailure == null) { lastFailure = e; } log.warn("Failed to stop subscription", e); }
                    }
                    synchronized (lifecycleLock) { status = Status.STOPPED; }
                }
            }
        };
    }

    private boolean ifExceptionCannotRetry(Throwable exception) {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause instanceof com.taobao.drc.client.checkpoint.CheckpointTrackingException
                    || cause instanceof com.taobao.drc.client.checkpoint.UnsafeRecoveryException
                    || cause instanceof DStoreOffsetNotExistException) { return true; }
        }
        if (exception != null && exception.getMessage() != null
           &&exception.getMessage().contains(" Found no feasible stores")
           && exception.getMessage().contains("data range available: from")) {
            return true;
        } else {
            return false;
        }
    }

    protected StoreClient switchClient(boolean isMulti) {
        CheckpointManager checkpointManager = new CheckpointManager(isMulti);
        SubscribeChannel oldChannel = clientChannel;
        SubscribeChannel channel = ClusterManagerFacade.askSubscribeChannelAndRedirect(userConfig);
        userConfig.setSubscribeChannel(channel);
        ClusterManagerFacade.askForCmUrl(userConfig, channel);
        StoreClient tmpClient = this.client;
        //Close the old client service.
        if (null != tmpClient) {
            try {
                tmpClient.stopService();
            } catch (Throwable e) {
                log.warn("Find subscription server switched, but stop old StoreClient failed." + e.getMessage());
            }
        }
        synchronized (lifecycleLock) {
            if (quit) { return null; }
            if (null == tmpClient || null == oldChannel || oldChannel != channel) {
                if (tmpClient != null && oldChannel != channel) {
                    String timestamp = initialCheckpoint == null ? null : initialCheckpoint.getTimestamp();
                    if (!StringUtils.isNumeric(timestamp)) {
                        throw new com.taobao.drc.client.checkpoint.UnsafeRecoveryException(
                                "Cannot migrate " + oldChannel + " to " + channel
                                + ": original start has no portable timestamp. Supply a verified timestamp with a new client.");
                    }
                    String reason = "Channel changed from " + oldChannel + " to " + channel
                            + "; protocol mapping is unverified. Replaying from original timestamp " + timestamp;
                    log.warn(reason);
                    checkpointManager.setRecoveryReason(reason);
                    userConfig.setCheckpoint(new Checkpoint(null, null, null, timestamp));
                }
                switch (channel) {
                    case DTS:
                        tmpClient = new DStoreClientImpl(listener, userConfig, checkpointManager, filter, parseThreadPrefix, notifyThreadPrefix);
                        break;
                    default:
                        tmpClient = new DRCStoreClientImpl(listener, userConfig, checkpointManager, filter, parseThreadPrefix, notifyThreadPrefix);
                        break;
                }
            }
            this.client = tmpClient;
            clientChannel = channel;
            log.info("Subscription channel=" + channel + ", multi=" + isMulti + ", explicit ACK required=" + isMulti);
            return tmpClient;
        }
    }

    /**
     * Re-enable start after join, retaining the confirmed state of this subscription.
     *
     * @throws Exception
     */
    @Override
    @Deprecated
    public void resetService() throws Exception {
        synchronized (lifecycleLock) {
            if (status == RAW || (worker != null && worker.isAlive()) || ListenerContext.isCallbackThread()) {
                throw new IllegalStateException("resetService requires the previous start thread to have exited; join it first");
            }
            quit = false;
            status = INIT;
        }
    }

    /**
     * Request cancellation; external callers also join the coordinator. Listener callbacks
     * must return without joining, so that the coordinator can finish shutting them down.
     */
    @Override
    public void stopService() throws Exception {
        StoreClient stopping;
        Thread joining;
        synchronized (lifecycleLock) {
            quit = true;
            stopping = client;
            joining = worker;
        }
        if (joining != null && joining != Thread.currentThread()) { joining.interrupt(); }
        if (stopping != null) { stopping.stopService(); }
        if (joining != null && joining != Thread.currentThread() && !ListenerContext.isCallbackThread()) {
            boolean interrupted = false;
            while (joining.isAlive()) {
                try { joining.join(); } catch (InterruptedException e) { interrupted = true; }
            }
            if (interrupted) { Thread.currentThread().interrupt(); }
        }
    }

    /**
     * Add listeners.
     */
    @Override
    public final void addListener(Listener listener) {
        this.listener = listener;
    }

    @Override
    public String getInstance() {
        return userConfig.getCheckpoint().getServerId();
    }

    @Override
    public void trimLongType() {
        userConfig.setTrimToLong(true);
    }


    private Checkpoint getLocalInfo(final String fileName) throws Exception {

        Checkpoint checkpoint;
        LocalityFile localityFile = new LocalityFile(fileName, 0, 0);
        String line, location = null;
        while ((line = localityFile.readLine()) != null) {
            if (isLocationInfo(line)) {
                location = extractLocationInfo(line);
            }
        }
        if (location == null || location.isEmpty()) {
            throw new Exception("Local file " + fileName + " not exists or is empty");
        }
        checkpoint = new Checkpoint();
        String items[] = location.split(":");
        checkpoint.setServerId(items[1] + "-" + items[2]);
        checkpoint.setPosition(items[4] + "@" + items[3]);
        checkpoint.setTimestamp(items[5]);
        if (items.length > 6)
            checkpoint.setRecordId(items[6]);
        return checkpoint;
    }

    private static boolean isLocationInfo(final String line) {
        if (line.contains(POSITION_INFO)) {
            return true;
        } else {
            return false;
        }
    }

    private static final String extractLocationInfo(final String string) {
        if (string == null) {
            return null;
        }
        String[] items = string.split(" ");
        return items[2];
    }

    @Override
    public final String getDbName() {
        return userConfig.getSubTopic();
    }

    @Override
    public void addDataFilter(DataFilterBase filter) {
        this.filter = filter;
    }

    @Override
    public final void setSqlEngine(SqlEngine engine) {
        userConfig.setSqlEngine(engine);
    }

    @Deprecated
    public final void addWhereFilter(DataFilter filter) {

    }

    @Deprecated
    public final void useStrictFilter() {

    }

    @Override
    public String getTaskName() {
        return "0";
    }

    /**
     * Set the frequency of heartbeat records received, one heartbeat every seconds.
     *
     * @param everySeconds after how many seconds, has been sent one heartbeat.
     */
    @Override
    @Deprecated
    public void setHeartbeatFrequency(int everySeconds) {
    }

    /**
     * Set whether requiring record type begin/commit.
     *
     * @param required ture if required, false otherwise, default is true.
     */
    public void requireTxnMark(boolean required) {
        userConfig.setRequireTx(required);
    }

    @Override
    public void filterEmptyTxnMark(boolean filterEmptyTxnMark) {
        userConfig.setFilterEmptyTxnMark(filterEmptyTxnMark);
    }

    @Override
    @Deprecated
    public void setNotifyRuntimePeriodInSec(long sec) {
    }

    @Override
    @Deprecated
    public void setNumOfRecordsPerBatch(int threshold) {
    }

    @Override
    @Deprecated
    public DBType getDatabaseType() {
        return DBType.MYSQL;
    }

    @Override
    @Deprecated
    public void setGroup(String group) {

    }

    @Override
    @Deprecated
    public void setSubGroup(String subGroup) {

    }

    @Override
    public void setDrcMark(String mark) {
        userConfig.setFilterInfo(mark);
    }

    @Override
    public void setDtsMark(String mark) {
        userConfig.setDtsMark(mark);
    }

    @Override
    public void askSelfUnit() {
        userConfig.setAskSelfUnit(true);
    }

    @Override
    public void setBlackList(String blackList) {
        userConfig.setBlackList(blackList);
    }

    @Override
    public void suspend() {

    }

    @Override
    public void resume() {

    }

    @Override
    public void usePublicIp() {
        userConfig.setUsePublicIp(true);
    }

    public void useCaseSensitive() {
        userConfig.setCaseSensitive(true);
    }

    @Deprecated
    public void addWhereFilter(DataFilterBase filter) {

    }

    @Override
    public void useCRC32Check() {
        userConfig.setCrc32Check(true);
    }

    @Override
    public void shutdownAutoRetry() {
        autoRetry = false;
    }

    @Override
    public void needUKRecord() {
        userConfig.setNeedUKRecord(true);
    }

    @Override
    public String getMultiSafeTimestamp() {
        StoreClient current = client;
        return current == null ? null : current.getMultiSafeTimestamp();
    }

    @Override public Throwable getLastFailure() { return lastFailure; }

    @Override
    public Map<String, CheckpointManager.Status> getCheckpointStatus() {
        StoreClient current = client;
        return current == null ? java.util.Collections.<String, CheckpointManager.Status>emptyMap()
                : current.getCheckpointStatus();
    }

    @Override
    public void setParseThreadPrefix(String prefix) {
        this.parseThreadPrefix=prefix;
    }

    @Override
    public void setNotifyThreadPrefix(String prefix) {
        this.notifyThreadPrefix=prefix;
    }

    @Override
    @Deprecated
    public Future writeUserCtlMessage(byte[] userCtlBytes) {
        if (null == client) {
            throw new IllegalStateException("StoreClient not initiated yet!");
        }
        return client.writeUserCtlMessage(userCtlBytes);
    }

    @Override
    public DBType getDBType() {
        return client.getRunningDBType();
    }

    @Override
    public void askOtherUnitUseThreadID() {
        userConfig.askOtherUnitUseThreadID();
    }

    @Override
    public void askSelfUnitUseThreadID() {
        userConfig.askSelfUnitUseThreadID();
    }
}
