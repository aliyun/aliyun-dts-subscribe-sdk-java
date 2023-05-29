package com.taobao.drc.client.utils;

import com.taobao.drc.client.config.UserConfig;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.nio.charset.Charset;
import java.util.Set;

/**
 * Created by jianjundeng on 5/7/15.
 */
public class Constant {
    public static final String DTS_CLIENT_VERSION = "1.1.8";

    public static final String DEFAULT_ENCODING = "UTF8";

    public static final String DEFAULT_FIELD_ENCODING = "ASCII";

    public static final Charset DEFAULT_CHARSET = Charset.forName(DEFAULT_ENCODING);

    public static final byte special_n = '\n';

    public static final byte kv_split = ':';

    public static final AttributeKey<UserConfig> configKey = AttributeKey.valueOf("userConfig");

    public static final AttributeKey<Set<Channel>> channelSetKey = AttributeKey.valueOf("channelSet");

    public static final AttributeKey metaCacheKey=AttributeKey.valueOf("metaCache");

    public static final String PRIM_META = "0";

    public static final String PRIM_INST = "";

    public static final String PRIM_TKNM = "";

    public static final String DELM = ":";

    public final static String POSITION_INFO = "Global_position_info:";

    public final static String CM_URL="manager.host";

    public final static String USE_DRCNET="useDrcNet";

    public final static String SERVER_MESSAGE_TYPE="server.messageType";

    public final static String BINARY_TYPE="binary";

    public final static String CHECKPOINT_PERIOD="checkpoint.period";

    public final static String NEED_RECORD_CACHE="client.requireCompleteTxn";

    public final static String MAX_NUM_RECORDS_PER_MESSAGE="client.maxNumOfRecordsPerMessage";

    public final static String MAX_TIMEOUT_PER_MESSAGE="client.maxTimeoutPerMessage";

    public final static String MAX_NUM_TX_PER_MESSAGE="client.maxNumOfTxnsPerMessage";

    public final static String MAX_NUM_RECORDS_CACHED="client.maxNumOfRecordsCached";

    public final static String REGION_NO_BLACK_LIST="client.regionNoBlackList";

    public final static String CLIENT_HASH_MASK="client.hashMask";

    public final static String CLIENT_HASH_MASK_EXPONENT="client.hashMaskExponent";

    public final static String CLIENT_HASH_KEY="client.hashKey";

    public final static String CLIENT_MAX_PENDING_RECORDS = "client.maxPendingRecords";

    public final static String CLIENT_READ_TIMEOUT_SECONDS = "client.readTimeoutSeconds";

    public final static String SERVER_MAX_RETRIED_TIMES = "server.maxRetriedTimes";

    public final static String CLIENT_MULTISERVICE_RETRY = "client.multiService.retry";

    public final static String REGION_CODE = "regionCode";

    public final static String REVERSED = "reversed";

    public final static String CLIENT_IP = "clientIp";

    public final static String SKIP_AUTH = "skipAuth";

    public final static String MAX_BYTES_RECORDS_CACHED = "client.maxBytesOfRecordsCached";

    public final static String DATA_FLOW_RPS_LIMIT = "client.dataFlowRpsLimit";

    /**
     * dstore fetch params
     */
    public final static String FETCH_WAIT_MS = "fetch.max.wait.ms";
    public final static String POLL_TIMEOUT_MS = "poll.timeout.ms";
    public final static String SASL_MECHANISM = "sasl.mechanism";
    public final static String SECURITY_PROTOCOL = "security.protocol";
    public final static String POLL_RECORDS_SIZE = "poll.records.size";
    public final static String POLL_BYTES_SIZE = "poll.bytes.size";
    public final static String SOCKET_TIMEOUT_MS = "socket.timeout.ms";
    public final static String FILTER_FNMATCH = "filter.fnmatch";

}
