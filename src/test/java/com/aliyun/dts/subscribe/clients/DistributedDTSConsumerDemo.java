package com.aliyun.dts.subscribe.clients;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.dms.subscribe.clients.DBMapper;
import com.aliyun.dms.subscribe.clients.DistributedConsumer;
import com.aliyun.dms.subscribe.clients.DefaultDistributedConsumer;


import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.recordprocessor.DbType;
import com.aliyun.dts.subscribe.clients.recordprocessor.DefaultRecordPrintListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class DistributedDTSConsumerDemo {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedDTSConsumerDemo.class);

    private final DistributedConsumer dmsConsumer;
    private String dProxy = "";
    private String checkpoint = "";
    private Map<String, String> topic2checkpoint;
    private String sid;
    String uid = "";
    String bid = "";

    static   ArrayList<String> topics = new ArrayList<>();
    static ArrayList<String> sids = new ArrayList<>();
    static ArrayList<String> dbLists = new ArrayList<>();
    static   ArrayList<String> subMigrationJobIds = new ArrayList<>();
    public DistributedDTSConsumerDemo(String username, String password, String host,
                                      ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint, boolean mapping) {
        this.dmsConsumer = initDMSConsumer(username, password, host, subscribeMode, isForceUseInitCheckpoint, mapping);

    }

    private DistributedConsumer initDMSConsumer(String username, String password, String host,
                                                ConsumerContext.ConsumerSubscribeMode subscribeMode, boolean isForceUseInitCheckpoint, boolean mapping) {
        DefaultDistributedConsumer dmsConsumer = new DefaultDistributedConsumer();


//        this.topic2checkpoint = vo.getTopics2checkpoint();
        this.topic2checkpoint = new HashMap<>();
        for (String topic: topics) {
            this.topic2checkpoint.put(topic, checkpoint);
        }

        for (String dbList: dbLists) {
            DBMapper.init(dbList);
        }

        dmsConsumer.init(topic2checkpoint, dProxy, sids, username, password,  subscribeMode, isForceUseInitCheckpoint,
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
        dmsConsumer.start();
    }


    public static void getSubscribeSubJobs() throws ClientException {
        DefaultProfile profile = DefaultProfile.getProfile("cn-hangzhou", "", "");
        IAcsClient client = new DefaultAcsClient(profile);
        DescribeDtsJobsRequest request = new DescribeDtsJobsRequest();




        request.setGroupId("");

        request.setJobType("subscribe");
        request.setRegion("cn-hangzhou");

        DescribeDtsJobsResponse response = client.getAcsResponse(request);
        subMigrationJobIds.addAll(response.getDtsJobList().stream().map(DescribeDtsJobsResponse.DtsJobStatus::getDtsJobId).collect(Collectors.toList()));
        System.out.println(new Gson().toJson(response));

    }

    public static   void getSubscriptionMeta() throws ClientException {

        DescribeSubscriptionMetaRequest req = new DescribeSubscriptionMetaRequest();
        req.setBisId("");
        req.setCallerBid("");
        req.setAction("DescribeSubscriptionMeta");
        req.setDtsJobId("");
        req.setSid("");
        req.setSubMigrationJobIds(subMigrationJobIds);

        DescribeSubscriptionMetaResponse res = subscriptionController.DescribeSubscriptionMeta(req);
        if (res.isSuccess()) {
            for (SubscriptionMeta meta: (res).getSubscriptionMetaList()) {
                topics.add(meta.getTopic());
                sids.add(meta.getSid());
                dbLists.add(meta.getDbList());
            }
        }

        JSONObject res = (JSONObject)JSONObject.parse(response);

        if ((res.get("success").equals(true))) {
            JSONArray metaList = res.getJSONArray("subscriptionMetaList");

            for (int i = 0; i < metaList.size(); i++) {
                JSONObject meta = metaList.getJSONObject(i);
                topics.add(meta.getString("topic"));
                sids.add(meta.getString("sid"));
                dbLists.add(meta.getString("dbList"));
            }
        }
    }

    public static void main(String[] args) {
        getSubscribeSubJobs();
        getSubscriptionMeta();

        String userName = "";
        String password = "";
        String host = "";
        String topicGroup = "";
        boolean mapping = true;



        ConsumerContext.ConsumerSubscribeMode subscribeMode = ConsumerContext.ConsumerSubscribeMode.ASSIGN;
        boolean isForceUseInitCheckpoint = false;

        DistributedDTSConsumerDemo demo = new DistributedDTSConsumerDemo(userName, password, host,  subscribeMode,
                isForceUseInitCheckpoint, mapping);
        demo.start();
    }
}
