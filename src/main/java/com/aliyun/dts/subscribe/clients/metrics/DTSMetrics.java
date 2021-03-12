package com.aliyun.dts.subscribe.clients.metrics;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;

import java.util.concurrent.TimeUnit;

public class DTSMetrics {

    private Metrics coreMetrics;

    public DTSMetrics() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.timeWindow(5, TimeUnit.SECONDS);
        metricConfig.samples(2);

        coreMetrics = new Metrics(metricConfig);

        LogMetricsReporter fileMetricsReporter = new LogMetricsReporter();
        fileMetricsReporter.configure(5, null);

        coreMetrics.addReporter(fileMetricsReporter);
        fileMetricsReporter.start();
    }

    public Metrics getCoreMetrics() {
        return coreMetrics;
    }

    public void setCoreMetrics(Metrics coreMetrics) {
        this.coreMetrics = coreMetrics;
    }

    public void close() {
        coreMetrics.close();
    }
}
