package com.aliyun.dts.subscribe.clients;

import com.aliyun.dms.subscribe.clients.DBMapper;
import com.aliyun.dms.subscribe.clients.DistributedDTSConsumer;
import com.aliyun.dms.subscribe.clients.DefaultDistributedDTSConsumer;

import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.recordprocessor.DbType;
import com.aliyun.dts.subscribe.clients.recordprocessor.DefaultRecordPrintListener;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.dts.model.v20200101.DescribeDtsJobsRequest;
import com.aliyuncs.dts.model.v20200101.DescribeDtsJobsResponse;
import com.aliyuncs.dts.model.v20200101.DescribeSubscriptionMetaRequest;
import com.aliyuncs.dts.model.v20200101.DescribeSubscriptionMetaResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class DistributedDTSConsumerDemo {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedDTSConsumerDemo.class);

    private final DistributedDTSConsumer distributedDTSConsumer;
    private String dProxy = "";
    private String checkpoint = "";
    private static Map<String, String> topic2checkpoint = new HashMap<>();
    private static Map<String, String> topic2Sid = new HashMap<>();
    private static ArrayList<String> dbLists = new ArrayList<>();

    public DistributedDTSConsumerDemo(String username, String password,
                                      ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint, boolean mapping) {
        DBMapper.setMapping(mapping);
        for (String dbList: dbLists) {
            DBMapper.init(dbList);
        }

        this.distributedDTSConsumer = initDMSConsumer(username, password, subscribeMode, isForceUseInitCheckpoint);
    }

    private DistributedDTSConsumer initDMSConsumer(String username, String password,
                                                   ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint) {

        DefaultDistributedDTSConsumer dmsConsumer = new DefaultDistributedDTSConsumer();
        // user can change checkpoint if needed
        for (String topic: topic2Sid.keySet()) {
            topic2checkpoint.put(topic, checkpoint);
        }

        dmsConsumer.init(topic2checkpoint, dProxy, topic2Sid, username, password,  subscribeMode, isForceUseInitCheckpoint,
                new UserMetaStore(), buildRecordListener());


        return dmsConsumer;
    }
    public static Map<String, RecordListener> buildRecordListener() {
        // user can impl their own listener
        RecordListener mysqlRecordPrintListener = new RecordListener() {
            @Override
            public void consume(DefaultUserRecord record) {

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
        distributedDTSConsumer.start();
    }


    public static void getSubscribeSubJobs(String region, String groupId, String sid, String dtsInstanceId) throws ClientException {
        DefaultProfile profile = DefaultProfile.getProfile("", "", "");
        IAcsClient client = new DefaultAcsClient(profile);
        DescribeDtsJobsRequest request = new DescribeDtsJobsRequest();

        request.setGroupId(groupId);
        request.setJobType("subscribe");
        request.setRegion(region);

        DescribeDtsJobsResponse response = client.getAcsResponse(request);
        List<String> subMigrationJobIds = response.getDtsJobList().stream().map(DescribeDtsJobsResponse.DtsJobStatus::getDtsJobId).collect(Collectors.toList());

        DescribeSubscriptionMetaRequest req = new DescribeSubscriptionMetaRequest();
        req.setSid(sid);
        req.setSubMigrationJobIds(String.join(",", subMigrationJobIds));

        req.setDtsInstanceId(dtsInstanceId);

        DescribeSubscriptionMetaResponse res = client.getAcsResponse(req);
        if (res.getSuccess().equalsIgnoreCase("true")) {
            for (DescribeSubscriptionMetaResponse.SubscriptionMetaListItem meta: (res).getSubscriptionMetaList()) {
                topic2Sid.put(meta.getTopic(), meta.getSid());
                dbLists.add(meta.getDBList());
            }
        }
    }

    public static void main(String[] args) throws ClientException {
        String region = "";
        String groupId = "";
        String sid = "";
        String dtsInstanceId = "";
        getSubscribeSubJobs(region, groupId, sid, dtsInstanceId);

        String userName = "";
        String password = "";
        //   String topicGroup = "";
        boolean mapping = true;

        ConsumerContext.ConsumerSubscribeMode subscribeMode = ConsumerContext.ConsumerSubscribeMode.ASSIGN;
        boolean isForceUseInitCheckpoint = false;

        DistributedDTSConsumerDemo demo = new DistributedDTSConsumerDemo(userName, password,  subscribeMode,
                isForceUseInitCheckpoint, mapping);
        demo.start();
    }
}
