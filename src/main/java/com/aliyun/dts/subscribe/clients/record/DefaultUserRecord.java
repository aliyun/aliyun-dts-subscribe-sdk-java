package com.aliyun.dts.subscribe.clients.record;

import com.aliyun.dts.subscribe.clients.common.UserCommitCallBack;
import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.function.Function;

public class DefaultUserRecord implements UserRecord {
    private final TopicPartition topicPartition;
    private final long offset;
    private final Record avroRecord;
    private final UserCommitCallBack userCommitCallBack;

    private volatile boolean initHeader = false;

    private RecordSchema recordSchema;
    private RowImage beforeImage;
    private RowImage afterImage;

    public DefaultUserRecord(TopicPartition tp, long offset, Record avroRecord, UserCommitCallBack userCommitCallBack) {
        this.topicPartition = tp;
        this.offset = offset;
        this.avroRecord = avroRecord;
        this.userCommitCallBack = userCommitCallBack;
    }

    public long getOffset() {
        return offset;
    }

    private <R> R callAvroRecordMethod(Function<? super Record, ? extends R> method) {
        return method.apply(avroRecord);
    }

    public com.aliyun.dts.subscribe.clients.formats.avro.Record getAvroRecord() {
        return avroRecord;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public void commit(String metadata) {
        userCommitCallBack.commit(topicPartition, getSourceTimestamp(), offset, metadata);
    }

    @Override
    public long getId() {
        return callAvroRecordMethod(Record::getId);
    }

    @Override
    public long getSourceTimestamp() {
        return callAvroRecordMethod(Record::getSourceTimestamp);
    }

    @Override
    public OperationType getOperationType() {
        return callAvroRecordMethod(AvroRecordParser::getOperationType);
    }

    @Override
    public RecordSchema getSchema() {
        return callAvroRecordMethod((avroRecord) -> {
            if (recordSchema == null) {
                recordSchema = AvroRecordParser.getRecordSchema(avroRecord);
            }
            return recordSchema;
        });
    }

    @Override
    public RowImage getBeforeImage() {
        if (null == beforeImage) {
            beforeImage = callAvroRecordMethod(record -> AvroRecordParser.getRowImage(getSchema(), record, true));
        }
        return beforeImage;
    }

    @Override
    public RowImage getAfterImage() {
        if (null == afterImage) {
            afterImage = callAvroRecordMethod(record -> AvroRecordParser.getRowImage(getSchema(), record, false));
        }
        return afterImage;
    }

    @Override
    public Map<String, String> getExtendedProperty() {
        return avroRecord.getTags();
    }
}
