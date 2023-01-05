package com.aliyun.dts.subscribe.clients.check.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class NodeCommandClientConfig extends AbstractConfig {

    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnect_back_off_time";
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;
    public static final String POLL_MS_CONFIG = "poll.ms";
    public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    private static final ConfigDef CONFIG;
    static {
        CONFIG = new ConfigDef()
                .define(BOOTSTRAP_SERVERS_CONFIG, // required with no default value
                        ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH,
                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                .define(METRICS_NUM_SAMPLES_CONFIG,
                        ConfigDef.Type.INT,
                        2,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        30000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                .define(METRIC_REPORTER_CLASSES_CONFIG,
                        ConfigDef.Type.LIST,
                        "",
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        9 * 60 * 1000,
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                .define(METADATA_MAX_AGE_CONFIG,
                        ConfigDef.Type.LONG,
                        5 * 60 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METADATA_MAX_AGE_DOC)
                .define(RECEIVE_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        32 * 1024,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RECEIVE_BUFFER_DOC)
                .define(RECONNECT_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        50L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        1000L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        "reconnect back off ms")
                .define(RETRY_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        100L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                .define(REQUEST_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        400 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
                .define(CLIENT_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.CLIENT_ID_DOC)
                .define(SECURITY_PROTOCOL_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .define(SEND_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        128 * 1024,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.SEND_BUFFER_DOC)
                .define(POLL_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        5000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        "pool time out from server");
    }
    public NodeCommandClientConfig(Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }


}