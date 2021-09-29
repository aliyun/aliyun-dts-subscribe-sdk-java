package com.aliyun.dts.subscribe.clients;

import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import com.aliyun.dts.subscribe.clients.exception.CriticalException;
import com.aliyun.dts.subscribe.clients.common.WorkThread;
import com.aliyun.dts.subscribe.clients.recordfetcher.KafkaRecordFetcher;
import com.aliyun.dts.subscribe.clients.recordgenerator.UserRecordGenerator;
import com.aliyun.dts.subscribe.clients.recordprocessor.EtlRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.dts.subscribe.clients.common.Util.*;

import java.util.LinkedList;
import java.util.List;

public class DefaultDTSConsumer extends AbstractDTSConsumer {
    private static final Logger log = LoggerFactory.getLogger(DefaultDTSConsumer.class);



    public DefaultDTSConsumer(ConsumerContext consumerContext) {
        super(consumerContext);
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

}
