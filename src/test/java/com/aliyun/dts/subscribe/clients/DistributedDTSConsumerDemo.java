package com.aliyun.dts.subscribe.clients;

import com.aliyun.dms.subscribe.clients.DBMapper;
import com.aliyun.dms.subscribe.clients.DistributedDTSConsumer;
import com.aliyun.dms.subscribe.clients.DefaultDistributedDTSConsumer;

import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.record.UserRecord;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class DistributedDTSConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(DistributedDTSConsumerDemo.class);

    private final DistributedDTSConsumer distributedDTSConsumer;
    private Map<String, String> topic2checkpoint = new HashMap<>();
    private Map<String, String> topic2Sid = new HashMap<>();
    private ArrayList<String> dbLists = new ArrayList<>();
    private DBMapper dbMapper = new DBMapper();

    public DistributedDTSConsumerDemo(String username, String password, String region, String groupId, String sid, String dtsInstanceId,
                                      String accessKeyId, String accessKeySecret, ConsumerContext.ConsumerSubscribeMode subscribeMode, String dProxy,
                                      String checkpoint, boolean isForceUseInitCheckpoint, boolean mapping) throws ClientException{
        getSubscribeSubJobs(region, groupId, sid, dtsInstanceId, accessKeyId, accessKeySecret);

        dbMapper.setMapping(mapping);
        dbMapper.init(dbLists);
        log.debug("init dbList:" + dbLists);
        this.distributedDTSConsumer = initDistributedConsumer(username, password, subscribeMode, dProxy, checkpoint, isForceUseInitCheckpoint);

    }

    private DistributedDTSConsumer initDistributedConsumer(String username, String password,
                                                   ConsumerContext.ConsumerSubscribeMode subscribeMode, String dProxy,
                                                   String checkpoint, boolean isForceUseInitCheckpoint) {

        DefaultDistributedDTSConsumer distributedConsumer = new DefaultDistributedDTSConsumer();
        // user can change checkpoint if needed
        for (String topic : topic2Sid.keySet()) {
            topic2checkpoint.put(topic, checkpoint);
        }

        distributedConsumer.init(topic2checkpoint, dbMapper, dProxy, topic2Sid, username, password, subscribeMode, isForceUseInitCheckpoint,
                new UserMetaStore(), buildRecordListener());

        return distributedConsumer;
    }

    public static Map<String, RecordListener> buildRecordListener() {
        // user can impl their own listener
        RecordListener mysqlRecordPrintListener = new RecordListener() {
            @Override
            public void consume(UserRecord record) {

                OperationType operationType = record.getOperationType();

                if (operationType.equals(OperationType.INSERT)
                        || operationType.equals(OperationType.UPDATE)
                        || operationType.equals(OperationType.DELETE)
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


    public void getSubscribeSubJobs(String region, String groupId, String sid, String dtsInstanceId, String accessKeyId, String accessKeySecret) throws ClientException {
        DefaultProfile profile = DefaultProfile.getProfile(region, accessKeyId, accessKeySecret);
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
            for (DescribeSubscriptionMetaResponse.SubscriptionMetaListItem meta : (res).getSubscriptionMetaList()) {
                topic2Sid.put(meta.getTopic(), meta.getSid());
                dbLists.add(meta.getDBList());

                if (StringUtils.isEmpty(meta.getDBList())) {
                    log.warn("dbList is null, sid:" + sid + ",dtsInstanceId:" + dtsInstanceId + ",subMigrationJobIds:" + String.join(",", subMigrationJobIds));
                }
            }
        }
        dbMapper.setClient(client);
        dbMapper.setDescribeSubscriptionMetaRequest(req);
    }

    public static void main(String[] args) throws ClientException {
        //分布式类型数据源的订阅配置方式，例如PolarDBX10(原DRDS)。配置AccessKey、实例Id、主任务id，订阅消费组等相关信息。
        String accessKeyId = "your access key id";
        String accessKeySecret = "your access key secret";
        String regionId = "your regionId";
        String dtsInstanceId = "your dts instanceId";
        String jobId = "your dts jobId";
        String sid = "your sid";
        String userName = "your user name";
        String password = "your password";
        String proxyUrl = "your proxyUrl";
        // initial checkpoint for first seek(a timestamp to set, eg 1566180200 if you want (Mon Aug 19 10:03:21 CST 2019))
        String checkpoint = "";

        // Convert physical database/table name to logical database/table name
        boolean mapping = true;
        // if force use config checkpoint when start. for checkpoint reset, only assign mode works
        boolean isForceUseInitCheckpoint = false;

        ConsumerContext.ConsumerSubscribeMode subscribeMode = ConsumerContext.ConsumerSubscribeMode.ASSIGN;
        DistributedDTSConsumerDemo demo = new DistributedDTSConsumerDemo(userName, password, regionId,
                jobId, sid, dtsInstanceId, accessKeyId, accessKeySecret, subscribeMode, proxyUrl,
                checkpoint, isForceUseInitCheckpoint, mapping);
        demo.start();
    }
}
