package com.aliyun.dts.subscribe.clients.metrics;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.dts.subscribe.clients.common.ThreadFactoryWithNamePrefix;
import com.aliyun.dts.subscribe.clients.exception.DTSBaseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.codehaus.jackson.annotate.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LogMetricsReporter implements MetricsReporter {
    private static final Logger LOG = LoggerFactory.getLogger(LogMetricsReporter.class);

    private static final Logger METRICS_LOG =
            LoggerFactory.getLogger("log.metrics");

    private static final String METRICS_PREFIX = "metrics.";
    static final String METRICS_PERIOD_CONFIG = METRICS_PREFIX + "period.sec";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(METRICS_PERIOD_CONFIG, ConfigDef.Type.INT, ConfigDef.Importance.HIGH,
                    "The frequency at which metrics should be reported, in second");

    private Set<String> includeSet = new HashSet<>();
    private final Object lock = new Object();
    private final Map<MetricName, KafkaMetric> metrics = new LinkedHashMap<>();

    private int period;

    private final ScheduledThreadPoolExecutor executor;
    private final Time time;

    private static final Map<String, String> STATIC_KEY_MAPPER = new HashMap<>();

    static {
    }

    public LogMetricsReporter() {
        this(new SystemTime(), new ScheduledThreadPoolExecutor(1, new ThreadFactoryWithNamePrefix("subscribe-logMetricsReporter-")));
    }

    LogMetricsReporter(Time mockTime, ScheduledThreadPoolExecutor mockExecutor) {
        time = mockTime;
        executor = mockExecutor;
    }

    @Override
    public void init(List<KafkaMetric> initMetrics) {
        synchronized (lock) {
            for (KafkaMetric metric : initMetrics) {
                if (includeSet.isEmpty() || includeSet.contains(metric.metricName().name())) {
                    LOG.debug("Adding metric {}", metric.metricName());
                    metrics.put(metric.metricName(), metric);
                }
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (lock) {
            if (includeSet.isEmpty() || includeSet.contains(metric.metricName().name())) {
                LOG.debug("Updating metric {}", metric.metricName());
                metrics.put(metric.metricName(), metric);
            }
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        synchronized (lock) {
            LOG.debug("Removing metric {}", metric.metricName());
            metrics.remove(metric.metricName());
        }
    }

    public void start() {
        executor.scheduleAtFixedRate(new PeriodReporter(), period, period, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new DTSBaseException("Interrupted when shutting down LogMetricsReporter" + e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        LogMetricsReporterConfig config = new LogMetricsReporterConfig(CONFIG_DEF, configs);
        period = config.getInteger(METRICS_PERIOD_CONFIG);

        LOG.info("Configured Log MetricsReporter to report every {} seconds", period);
    }

    public void configure(int period, String includeCfg) {
        this.period = period;
        if (StringUtils.isBlank(includeCfg)) {
            return;
        }
        Collections.addAll(includeSet, includeCfg.split(","));
    }


    public String genContent(Collection<MetricValue> metricValues) {
        try {
            if (metricValues == null) {
                return null;
            }
            JSONObject jsonObject = new JSONObject();
            for (MetricValue metricValue : metricValues) {
                BigDecimal metricMeasurableValue = null;
                try {
                    metricMeasurableValue = new BigDecimal(String.valueOf(metricValue.value));
                } catch (Exception var8) {
                    // handle hot key info
                    if (StringUtils.equalsIgnoreCase(metricValue.name, "maxQueuedKeyInfo")) {
                        jsonObject.put("maxQueuedKeyInfo", String.valueOf(metricValue.value));
                    }
                    continue;
                }
                String alias = STATIC_KEY_MAPPER.get(metricValue.name);
                if (metricMeasurableValue.scale() > 2) {
                    metricMeasurableValue = metricMeasurableValue.setScale(2, RoundingMode.HALF_UP);
                }
                jsonObject.put(metricValue.name, metricMeasurableValue);
                if (null != alias) {
                    jsonObject.put(alias, metricMeasurableValue);
                }
            }

            jsonObject.put("__dt", (new Date()).getTime());
            return jsonObject.toJSONString();
        } catch (Exception var9) {
            LOG.warn("LogMetricsReporter: format metric failed, cause " + var9.getMessage());
        }
        return null;
    }

    private static class LogMetricsReporterConfig extends AbstractConfig {
        LogMetricsReporterConfig(ConfigDef definition, Map<?, ?> originals) {
            super(definition, originals);
        }

        public Integer getInteger(String key) {
            return (Integer) get(key);
        }
    }


    private class PeriodReporter implements Runnable {
        @Override
        public void run() {
            long now = time.milliseconds();
            final List<MetricValue> samples;
            synchronized (lock) {
                samples = new ArrayList<>(metrics.size());
                for (KafkaMetric metric : metrics.values()) {
                    MetricName name = metric.metricName();
                    samples.add(new MetricValue(name.name(), name.group(), name.tags(), metric.metricValue()));
                }
            }

            MetricsReport report = new MetricsReport(now, samples);
            LOG.trace("Reporting {} metrics", samples.size());
            String content = genContent(report.metrics());
            if (!StringUtils.isEmpty(content)) {
                METRICS_LOG.info(content);
            }
        }
    }

    private static class MetricValue {

        private final String name;
        private final String group;
        private final Map<String, String> tags;
        private final Object value;

        MetricValue(String name, String group, Map<String, String> tags, Object value) {
            this.name = name;
            this.group = group;
            this.tags = tags;
            this.value = value;
        }

        @JsonProperty
        public String name() {
            return name;
        }

        @JsonProperty
        public String group() {
            return group;
        }

        @JsonProperty
        public Map<String, String> tags() {
            return tags;
        }

        @JsonProperty
        public Object value() {
            return value;
        }

        @Override
        public String toString() {
            return String.format("name=%s;group=%s;tags=%s;value=%s", name, group, tags, value);
        }
    }

    private static class MetricsReport {
        private final long timestamp;
        private final Collection<MetricValue> metrics;

        MetricsReport(long timestamp, Collection<MetricValue> metrics) {
            this.timestamp = timestamp;
            this.metrics = metrics;
        }

        @JsonProperty
        public long timestamp() {
            return timestamp;
        }

        @JsonProperty
        public Collection<MetricValue> metrics() {
            return metrics;
        }
    }
}
