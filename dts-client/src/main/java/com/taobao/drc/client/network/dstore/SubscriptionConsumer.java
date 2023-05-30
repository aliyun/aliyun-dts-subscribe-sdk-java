package com.taobao.drc.client.network.dstore;

import com.aliyun.dts.subscribe.clients.recordfetcher.ClusterSwitchListener;
import com.taobao.drc.client.DataFilterBase;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.SubscribeChannel;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.cm.ClusterManagerFacade;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.message.dts.DStoreAvroRecord;
import com.taobao.drc.client.message.dts.record.OperationType;
import com.taobao.drc.client.store.impl.AbstractStoreClient;
import com.taobao.drc.togo.client.consumer.*;
import com.taobao.drc.togo.common.businesslogic.TagMatchFunction;
import com.taobao.drc.togo.client.consumer.FetchRule;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * TogoConsumer that using server data filter.
 * @author xusheng.zkw
 */
public class SubscriptionConsumer extends BaseDStoreConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SubscriptionConsumer.class);
    private volatile com.taobao.drc.togo.client.consumer.TogoConsumer consumer;
    private volatile FetchRule rule;
    private volatile Long lastOutOfRangeOffset = null;
    private volatile long lastReceivedTime = 0;
    private volatile long socketTimeOut = Long.MAX_VALUE;

    protected ClusterSwitchListener clusterSwitchListener = new ClusterSwitchListener();

    public SubscriptionConsumer(Listener listener) {
        super(listener);
    }

    @Override
    public void init(UserConfig userConfig, CheckpointManager checkpointManager) {
        super.init(userConfig, checkpointManager);
        newConsumer(userConfig, true);
        socketTimeOut = userConfig.getSocketTimeOut();
    }

    private void newConsumer(UserConfig userConfig, boolean configPosition) {
        this.consumer = new com.taobao.drc.togo.client.consumer.TogoConsumer(consumerConfig(userConfig));
        this.consumer.addClusterResourceListener(clusterSwitchListener);

        if (null == assignedPartition) {
            String topic = userConfig.getSubTopic();
            this.assignedPartition = new TopicPartition(topic, 0);
        }
        if (null == rule) {
            rule = getFetchRule(userConfig);
        }
        if (null != rule) {
            this.consumer.setFetchRule(assignedPartition, rule);
        }
        this.consumer.assign(Arrays.asList(assignedPartition));
        this.consumer.seek(assignedPartition, configPosition ? getPosition() : getFilterOutOfRangePosition());
        consumer.setFetchMatchFunction(userConfig.isFnMatchFilter() ? TagMatchFunction.FNMATCH : TagMatchFunction.REGEX);
    }

    /**
     * reset seek partition offset
     */
    private void reset() {
        try {
            consumer.close();
            consumer = null;
        } catch (Throwable e) {
            logger.warn("reset dstore consumer error:" + e.getMessage());
        }
        newConsumer(getConfig(), true);
    }

    @Override
    public List<DataMessage.Record> poll(long timeoutMs) throws InterruptedException {
        SchemafulConsumerRecords consumerRecords = null;
        try {
            consumerRecords = consumer.poll(timeoutMs);
        } catch (OffsetOutOfRangeException e) {
            Long oldOutOfRangeOffset = lastOutOfRangeOffset;
            lastOutOfRangeOffset = e.offsetOutOfRangePartitions().get(assignedPartition);
            String message = "[offset:" + (null != config.getCheckpoint().getOffset() ? config.getCheckpoint().getOffset() : -1) + "]"
                    + "[timestamp:" + config.getCheckpoint() + "][prevOutOfRangeOffset:" + (null != oldOutOfRangeOffset ? oldOutOfRangeOffset : -1) + "]"
                    + "[currentOutOfRangeOffset:" + (null != lastOutOfRangeOffset ? lastOutOfRangeOffset : -1) + "]" + e.getMessage();
            logger.warn(message);
            AbstractStoreClient.sendRuntimeLog(listener, "WARN", message);
            Thread.sleep(5000);
            reset();
        } catch (ClusterSwitchListener.ClusterSwitchException e) {
            logger.warn("Cluster switch exception, reset the consumer");
            Thread.sleep(5000);
            reset();
        }
        if (null == consumerRecords || consumerRecords.isEmpty()) {
            //If long time not got messages, Client will check dstore is ok or not.
            if ((System.currentTimeMillis() - lastReceivedTime) > socketTimeOut) {
                AbstractStoreClient.sendRuntimeLog(listener, "WARN", "[DTS]" + socketTimeOut + "ms no received messages, checking connection.");
                lastReceivedTime = System.currentTimeMillis();
                //May be throw org.apache.kafka.common.errors.TimeoutException: Timeout expired while fetching topic metadata
                //consumer.listTopics();
                if (ClusterManagerFacade.askSubscribeChannelAndRedirect(config) == SubscribeChannel.DRC) {
                    AbstractStoreClient.sendRuntimeLog(listener, "WARN", "Consumption switch to DRC");
                    throw new DStoreSwitchException("Consumption switch to DRC.");
                } else {
                    logger.error("Get consumer records empty, retrieved record count: " + consumerRecords.count());
                    reset();
                }
            }
            return Collections.emptyList();
        } else {
            lastReceivedTime = System.currentTimeMillis();
        }
        List<DataMessage.Record> records = new ArrayList<DataMessage.Record>(consumerRecords.count());
        Iterator<SchemafulConsumerRecord> iters = consumerRecords.iterator();
        while (iters.hasNext()) {
            SchemafulConsumerRecord consumerRecord = iters.next();
            DStoreAvroRecord tmp = new DStoreAvroRecord(consumerRecord.data(), consumerRecord.offset(), consumerRecord.topic());
            //NOOP
            if (tmp.getRawOp() == OperationType.NOOP.ordinal()) {
                continue;
            }
            if (null != consumerRecord.data()) {
                tmp.setRecordLength(consumerRecord.data().length);
            }
            records.add(tmp);

        }

        //filter emptyTxnMark while asked.
        if (config.isFilterEmptyTxnMark()) {
            /**
             * filter empty transaction couple
             */
            int recordsSize = records.size();
            List<DataMessage.Record> newRecords = new ArrayList<DataMessage.Record>(recordsSize);
            for (int i = 0; i < recordsSize; ) {
                DataMessage.Record record = records.get(i);
                //the next record
                DataMessage.Record nextRecord = (i + 1) < recordsSize ? records.get(i + 1) : null;
                //empty transaction couple
                if (record.getOpt() == DataMessage.Record.Type.BEGIN && null != nextRecord
                        && (nextRecord.getOpt() == DataMessage.Record.Type.ROLLBACK || nextRecord.getOpt() == DataMessage.Record.Type.COMMIT)
                ) {
                    i += 2;
                    continue;
                } else {
                    i++;
                }
                newRecords.add(record);
            }
            return newRecords;
        } else {
            return records;
        }
    }

    @Override
    public void doClose() {
        if (consumer != null) {
            lastOutOfRangeOffset = null;
            consumer.close();
            consumer = null;
        }
    }

    private long getFilterOutOfRangePosition() {
        return null != lastOutOfRangeOffset && lastOutOfRangeOffset > -1 ? lastOutOfRangeOffset : super.getPosition();
    }

    @Override
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return consumer.beginningOffsets(partitions);
    }

    @Override
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestamp) {
        return consumer.offsetsForTimes(timestamp);
    }

    /**
     * Create a FetchRule to filter data in DStore server.
     * @param config
     * @return
     */
    private FetchRule getFetchRule(UserConfig config) {
        FetchRules.FilterRuleBuilder filter = FetchRules.filter();
        DataFilterBase base = config.getDataFilter();
        Map<String, Map<String, List<String>>> rules = base.dstoreRequiredMap();
        //include
        if (null != rules && !rules.isEmpty()) {
            logger.info("rules size: " + rules.size());
            for (Map.Entry<String, Map<String, List<String>>> entry : rules.entrySet()) {
                String dbName = entry.getKey();
                for (String tb : entry.getValue().keySet()) {
                    String tableName = tb;
                    filter.include().add("default", "dbName", dbName).add("default", "tbName", tableName).done();
                }
            }
        }
        //exclude
        if (StringUtils.isNotEmpty(config.getBlackList())) {
            String[] segment = config.getBlackList().split("\\|");
            Map<String, List<String>> excludes = new HashMap<String, List<String>>();
            for (String dt : segment) {
                String[] dtPair = dt.split("[;,\\.]");
                String dbName = dtPair[0];
                String table = dtPair[1];
                List<String> tables = excludes.get(dbName);
                if (null == tables) {
                    tables = new ArrayList<String>();
                }
                tables.add(table);
                excludes.put(dbName, tables);
            }
            for (Map.Entry<String, List<String>> tb : excludes.entrySet()) {
                for (String tbName : tb.getValue()) {
                    filter.exclude().add("default", "dbName", tb.getKey()).add("default", "tbName", tbName).done();
                }
            }
        }

        //hash
        if(config.getHashMask() > 0){
            String[] hashKeys = config.getHashKey().split(",");
            List<Integer> formattedHashKeys = new ArrayList<Integer>(hashKeys.length);
            for (String hashKey : hashKeys) {
                formattedHashKeys.add(Integer.parseInt(hashKey));
            }
            filter.hashTypeFilterBuilder().maxHashValue(config.getHashMask()).addSubHashElements(formattedHashKeys).done();
        }
        //unit
        FetchRules.FilterRuleBuilder.UnitFilterTypeBuilder unitFilterTypeBuilder = filter.unitFilterTypeBuilder();
        boolean askAllUnit = true;
        boolean askGevinUnit = false;
        /*
         * regionId & threadid filter
         */
        if (StringUtils.isNotEmpty(config.getRegionNoBlackList())) {
            String[] regionArray =  config.getRegionNoBlackList().split(",");
            List<Integer> formattedRegion = new ArrayList<Integer>(regionArray.length);
            for (String region : regionArray) {
                formattedRegion.add(Integer.parseInt(region));
            }
            unitFilterTypeBuilder.askGivenUnit(formattedRegion, false);
            askAllUnit = false;
            askGevinUnit = true;
        }
        if (config.isAskSelfUnitUseThreadID()) {
            unitFilterTypeBuilder.askSelfUnitUseThreadID();
            askAllUnit = false;
        }
        if (config.isAskOtherUnitUseThreadID()) {
            unitFilterTypeBuilder.askOtherUnitUseThreadID();
            askAllUnit = false;
        }

        /*
         * txn filter
         */
        FetchRules.FilterRuleBuilder.UnitFilterTypeBuilder.TxnEntryBuilder txnEntryBuilder = null;
        if (StringUtils.trimToEmpty(config.getDtsMark()).startsWith("!")) {
            txnEntryBuilder = unitFilterTypeBuilder.askOtherUnitUseThreadID().askOtherUnitUseTxn();
            askAllUnit = false;
        }
        if (config.isAskSelfUnit()) {
            txnEntryBuilder = unitFilterTypeBuilder.askSelfUnitUseThreadID().askSelfUnitUseTxn();
            askAllUnit = false;
        }
        if (null != txnEntryBuilder) {
            String newFilterInfo = StringUtils.trimToEmpty(config.getDtsMark()).startsWith("!") ?
                    StringUtils.trimToEmpty(config.getDtsMark()).substring(1) : StringUtils.trimToEmpty(config.getDtsMark());
            String[] patterns = newFilterInfo.split("\\|");
            for (String pattern : patterns) {
                String db = StringUtils.substringBefore(pattern, ".");
                String table = StringUtils.substringAfter(pattern, ".");
                txnEntryBuilder.addTxnPattern(db, table);
            }
            txnEntryBuilder.done();
        }
        if (askAllUnit) {
            //default setting
            unitFilterTypeBuilder.askAllUnit();
        }
        //filter begin/commit
        if ((askAllUnit || askGevinUnit) && !config.isRequireTx()) {
            filter.specialRecordTypeBuilder().filterBeginCommit().done();
        }

        unitFilterTypeBuilder.done();
        FetchRule filterRule =  filter.build();

        logger.info("UnitTypeFilterTag: " + filterRule.unitTypeFilterTag());
        return filterRule;
    }
}
