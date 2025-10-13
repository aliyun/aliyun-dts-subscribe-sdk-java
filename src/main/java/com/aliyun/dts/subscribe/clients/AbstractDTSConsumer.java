package com.aliyun.dts.subscribe.clients;

import com.aliyun.dts.subscribe.clients.check.CheckManager;
import com.aliyun.dts.subscribe.clients.check.CheckResult;
import com.aliyun.dts.subscribe.clients.check.DefaultCheckManager;
import com.aliyun.dts.subscribe.clients.check.SubscribeNetworkChecker;
import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.common.WorkThread;
import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;
import com.aliyun.dts.subscribe.clients.recordfetcher.KafkaRecordFetcher;
import com.aliyun.dts.subscribe.clients.recordgenerator.UserRecordGenerator;
import com.aliyun.dts.subscribe.clients.recordprocessor.EtlRecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import static com.aliyun.dts.subscribe.clients.common.Util.require;
import static com.aliyun.dts.subscribe.clients.common.Util.swallowErrorClose;

public abstract class AbstractDTSConsumer implements DTSConsumer {
    private static final Logger log = LoggerFactory.getLogger(AbstractDTSConsumer.class);
    private static final Logger core_log = LoggerFactory.getLogger("log.metrics");

    protected ConsumerContext consumerContext;

    protected Map<String, RecordListener> recordListeners;

    protected final LinkedBlockingQueue<ConsumerRecord> toProcessRecords;
    protected final LinkedBlockingQueue<DefaultUserRecord> defaultUserRecords;

    protected volatile boolean started = false;;

    public AbstractDTSConsumer(ConsumerContext consumerContext) {
        this.consumerContext = consumerContext;
        core_log.info("consumerContext: " + consumerContext);
        this.toProcessRecords = new LinkedBlockingQueue<>(512);
        this.defaultUserRecords = new LinkedBlockingQueue<>(512);
    }

    @Override
    public abstract void start();

    private static List<WorkThread> startWorker(EtlRecordProcessor etlRecordProcessor, UserRecordGenerator userRecordGenerator, KafkaRecordFetcher recordGenerator) {
        List<WorkThread> ret = new LinkedList<>();
        ret.add(new WorkThread(etlRecordProcessor, EtlRecordProcessor.class.getName()));
        ret.add(new WorkThread(userRecordGenerator, UserRecordGenerator.class.getName()));
        ret.add(new WorkThread(recordGenerator, KafkaRecordFetcher.class.getName()));
        for (WorkThread workThread : ret) {
            workThread.start();
        }
        return ret;
    }

    @Override
    public void addRecordListeners(Map<String, RecordListener> recordListeners) {
        require(null != recordListeners && !recordListeners.isEmpty(), "record listener required");

        recordListeners.forEach((k, v) -> {
            log.info("register record listener " + k);
        });

        this.recordListeners = recordListeners;
    }

    @Override
    public boolean check() {
        CheckManager checkerManager = new DefaultCheckManager(consumerContext);

        checkerManager.addCheckItem(new SubscribeNetworkChecker(consumerContext.getBrokerUrl()));

        CheckResult checkResult = checkerManager.check();

        if (checkResult.isOk()) {
            log.info(checkResult.toString());
            return true;
        } else {
            log.error(checkResult.toString());
            return false;
        }
    }

    protected static Properties initLog4j() {
        Properties properties = new Properties();
        InputStream log4jInput = null;
        try {
            log4jInput = Thread.currentThread().getContextClassLoader().getResourceAsStream("log4j.properties");
            PropertyConfigurator.configure(log4jInput);
        } catch (Exception e) {
        } finally {
            swallowErrorClose(log4jInput);
        }
        return properties;
    }

    @Override
    public void close() {
        this.consumerContext.exit();

    }
}
