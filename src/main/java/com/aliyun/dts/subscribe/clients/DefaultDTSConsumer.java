package com.aliyun.dts.subscribe.clients;

import com.aliyun.dts.subscribe.clients.check.*;
import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.exception.CriticalException;
import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;
import com.aliyun.dts.subscribe.clients.common.WorkThread;
import com.aliyun.dts.subscribe.clients.recordfetcher.KafkaRecordFetcher;
import com.aliyun.dts.subscribe.clients.recordgenerator.UserRecordGenerator;
import com.aliyun.dts.subscribe.clients.recordprocessor.EtlRecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.dts.subscribe.clients.common.Util.*;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class DefaultDTSConsumer implements DTSConsumer {
    private static final Logger log = LoggerFactory.getLogger(DefaultDTSConsumer.class);

    private ConsumerContext consumerContext;

    private Map<String, RecordListener> recordListeners;

    private final LinkedBlockingQueue<ConsumerRecord> toProcessRecords;
    private final LinkedBlockingQueue<DefaultUserRecord> defaultUserRecords;

    private volatile boolean started = false;;

    public DefaultDTSConsumer(ConsumerContext consumerContext) {
        this.consumerContext = consumerContext;

        this.toProcessRecords = new LinkedBlockingQueue<>(512);
        this.defaultUserRecords = new LinkedBlockingQueue<>(512);
    }

    @Override
    public void start() {

        //check firstly
        boolean checkResult = check();

        if (!checkResult) {
            log.error("DTS precheck failed, dts consumer exit.");
            throw new CriticalException("DTS precheck failed, dts consumer exit.");
        }

        synchronized (this) {
            initLog4j();
            if (started) {
                throw new IllegalStateException("The client has already been started");
            }

            KafkaRecordFetcher recordFetcher = new KafkaRecordFetcher(consumerContext, toProcessRecords);

            UserRecordGenerator userRecordGenerator = new UserRecordGenerator(consumerContext, toProcessRecords, defaultUserRecords,
                    (tp, timestamp, offset, metadata) -> recordFetcher.setToCommitCheckpoint(new Checkpoint(tp, timestamp, offset, metadata)));

            //processor
            EtlRecordProcessor etlRecordProcessor = new EtlRecordProcessor(consumerContext, defaultUserRecords, recordListeners);

            List<WorkThread> startStream = startWorker(etlRecordProcessor, userRecordGenerator, recordFetcher);

            while (!consumerContext.isExited()) {
                sleepMS(1000);
            }
            log.info("DTS Consumer: shutting down...");
            for (WorkThread workThread : startStream) {
                workThread.stop();
            }

            started = true;
        }
    }

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

        checkerManager.addCheckItem(new SubscribeAuthChecker(consumerContext));

        CheckResult checkResult = checkerManager.check();

        if (checkResult.isOk()) {
            log.info(checkResult.toString());
            return true;
        } else {
            log.error(checkResult.toString());
            return false;
        }
    }

    private static Properties initLog4j() {
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
}
