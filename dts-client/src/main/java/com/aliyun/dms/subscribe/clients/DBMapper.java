package com.aliyun.dms.subscribe.clients;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.dts.subscribe.clients.common.RetryUtil;
import com.aliyun.dts.subscribe.clients.formats.avro.Operation;
import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.dts.model.v20200101.DescribeSubscriptionMetaRequest;
import com.aliyuncs.dts.model.v20200101.DescribeSubscriptionMetaResponse;
import com.aliyuncs.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DBMapper {
    private final Logger log = LoggerFactory.getLogger(DBMapper.class);

    private Map<String, String> physic2logicDBMapper = new HashMap<>();
    private Map<String, String> physic2logicTableMapper = new HashMap<>();
    private boolean mapping = true;

    private IAcsClient iAcsClient;
    private DescribeSubscriptionMetaRequest describeSubscriptionMetaRequest;

    private RetryUtil retryUtil = new RetryUtil(4, TimeUnit.SECONDS, 15, (e) -> true);

    public void setClient(IAcsClient client) {
        iAcsClient = client;
    }

    public void setDescribeSubscriptionMetaRequest(DescribeSubscriptionMetaRequest describeSubscriptionMetaRequest) {
        this.describeSubscriptionMetaRequest = describeSubscriptionMetaRequest;
    }

    public synchronized void init(String dbListString) {
        JSONObject dbList = JSONObject.parseObject(dbListString);
        for (Map.Entry<String, Object> entry: dbList.entrySet()) {
            String physicDb = entry.getKey();
            String logicDb = (String)((JSONObject)entry.getValue()).get("name");
            JSONObject tables = (JSONObject)((JSONObject)entry.getValue()).get("Table");

            physic2logicDBMapper.put(physicDb, logicDb);
            for (Map.Entry<String, Object> table: tables.entrySet()) {
                String physicTable = table.getKey();
                String logicTable = (String)((JSONObject)table.getValue()).get("name");
                physic2logicTableMapper.put(physicDb + "." + physicTable, logicDb + "." + logicTable);
            }
        }
    }

    public void init(List<String> dbLists) {
        for (String dbList: dbLists) {
            init(dbList);
        }
    }

    public boolean refreshDbList() throws ClientException {
        List<String> dbLists = new ArrayList<>();
        DescribeSubscriptionMetaResponse res = iAcsClient.getAcsResponse(this.describeSubscriptionMetaRequest);
        boolean success =  res.getSuccess().equalsIgnoreCase("true");
        if (success) {
            for (DescribeSubscriptionMetaResponse.SubscriptionMetaListItem meta: (res).getSubscriptionMetaList()) {
                dbLists.add(meta.getDBList());
                log.debug("refresh dbList:" + meta.getDBList());
            }
            init(dbLists);
        }
        return success;

    }

    public Record transform(Record record)  {
        // do not support ddl for now
//            if (record.getOperation().equals(Operation.DDL)) {
//                if (physic2logicDBMapper.containsKey(record.getObjectName())) {
//                    record.setObjectName(physic2logicDBMapper.get(record.getObjectName()));
//                }
//            }

        if (record.getOperation().equals(Operation.INSERT) || record.getOperation().equals(Operation.UPDATE) ||
                record.getOperation().equals(Operation.DELETE))  {
            if (!physic2logicTableMapper.containsKey(record.getObjectName())) {
                log.info("Cannot find logic db table for " + record.getObjectName() + ", refreshing dbList now");
                try {
                    retryUtil.callFunctionWithRetry(
                            () -> {
                                refreshDbList();
                            }
                    );
                }  catch (Exception e) {
                    log.error("Error getting dbList:" + e);
                }
            }
            record.setObjectName(physic2logicTableMapper.get(record.getObjectName()));
        }
        return record;
    }

    public boolean isMapping() {
        return mapping;
    }

    public void setMapping(boolean mapping) {
        this.mapping = mapping;
    }

}
