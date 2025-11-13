package com.aliyun.dts.subscribe.clients;

import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.record.UserRecord;
import com.aliyun.dts.subscribe.clients.recordprocessor.DbType;
import com.aliyun.dts.subscribe.clients.recordprocessor.DefaultRecordPrintListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class DTSConsumerAssignDemo {
    private static final Logger log = LoggerFactory.getLogger(DTSConsumerAssignDemo.class);

    private final DTSConsumer dtsConsumer;

    public DTSConsumerAssignDemo(String brokerUrl, String topic, String sid, String userName, String password,
                                 String checkpoint, ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint) {
        this.dtsConsumer = initDTSClient(brokerUrl, topic, sid, userName, password, checkpoint, subscribeMode, isForceUseInitCheckpoint);
    }

    private DTSConsumer initDTSClient(String brokerUrl, String topic, String sid, String userName, String password,
                                      String initCheckpoint, ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint) {
        ConsumerContext consumerContext = new ConsumerContext(brokerUrl, topic, sid, userName, password, initCheckpoint, subscribeMode);

        //if this parameter is set, force to use the initCheckpoint to initial
        consumerContext.setForceUseCheckpoint(isForceUseInitCheckpoint);

        //add user store
        consumerContext.setUserRegisteredStore(new UserMetaStore());

        DTSConsumer dtsConsumer = new DefaultDTSConsumer(consumerContext);

        dtsConsumer.addRecordListeners(buildRecordListener());

        return dtsConsumer;
    }

    public static Map<String, RecordListener> buildRecordListener() {
        // user can impl their own listener
        RecordListener mysqlRecordPrintListener = new RecordListener() {
            @Override
            public void consume(UserRecord record) {

                OperationType operationType = record.getOperationType();

                if(operationType.equals(OperationType.INSERT)
                        || operationType.equals(OperationType.UPDATE)
                        || operationType.equals(OperationType.DELETE)
                        || operationType.equals(OperationType.DDL)
                        || operationType.equals(OperationType.HEARTBEAT)) {

                    // consume record
                    RecordListener recordPrintListener = new DefaultRecordPrintListener(DbType.MySQL);

                    recordPrintListener.consume(record);

                    //commit method push the checkpoint update
                    record.commit("");
                }
            }
        };
        return Collections.singletonMap("mysqlRecordPrinter", mysqlRecordPrintListener);
    }

    public void start() {
        System.out.println("Start DTS subscription client...");

        dtsConsumer.start();
    }

    public static void main(String[] args) {
        // kafka broker url
        String brokerUrl = "your broker url";
        // topic to consume, partition is 0
        String topic = "your dts topic";
        // user password and sid for auth
        String sid = "your sid";
        String userName = "your user name";
        String password = "your password";
        // initial checkpoint for first seek(a timestamp to set, eg 1566180200 if you want (Mon Aug 19 10:03:21 CST 2019))
        String initCheckpoint = "start timestamp";
        // when use subscribe mode, group config is required. kafka consumer group is enabled
        ConsumerContext.ConsumerSubscribeMode subscribeMode = ConsumerContext.ConsumerSubscribeMode.ASSIGN;
        // if force use config checkpoint when start. for checkpoint reset, only assign mode works
        boolean isForceUseInitCheckpoint = true;

        DTSConsumerAssignDemo consumerDemo = new DTSConsumerAssignDemo(brokerUrl, topic, sid, userName, password, initCheckpoint, subscribeMode, isForceUseInitCheckpoint);
        consumerDemo.start();
    }
}
