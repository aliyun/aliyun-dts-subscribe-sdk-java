package com.aliyun.dts.subscribe.clients.record.fast;

import com.aliyun.dts.subscribe.clients.common.UserCommitCallBack;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.record.RowImage;
import com.aliyun.dts.subscribe.clients.record.UserRecord;
import com.aliyun.dts.subscribe.clients.record.fast.util.TimeUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LazyParseRecordImpl implements UserRecord {

    private byte[] rawValue;
    private volatile boolean initHeader = false;
    private BufferWarpBinaryDecoder binaryDecoder;
    private long id;
    private long timestamp;
    private String sourcePosition;
    private String sourceSafePosition;
    private String transactionId;
    private OperationType operationType;
    private Pair<String, String> sourceTypeAndVersion;
    private int sourceTypeCode;
    private LazyRecordSchema recordSchema;
    private LazyRecordDeserializer deserializer;

    private Map<String, String> extendedProperty;
    // for delete, the pk and uk value list in before images
    private List<List<String>> prevKeys;
    // for insert and update,  the pk and uk value list in after images
    private List<List<String>> nextKeys;
    private final long offset;
    private long size;

    private volatile boolean initPayload = false;
    private RowImage beforeImage;
    private RowImage afterImage;

    private long bornTimestamp;

    private final TopicPartition topicPartition;

    private final UserCommitCallBack userCommitCallBack;

    public LazyParseRecordImpl(TopicPartition tp, byte[] recordData, long offset, LazyRecordDeserializer deserializer, UserCommitCallBack userCommitCallBack) {
        this.topicPartition = tp;

        this.rawValue = recordData;
        this.offset = offset;
        this.size = recordData.length;
        this.deserializer = deserializer;
        this.userCommitCallBack = userCommitCallBack;
    }

    protected void initHeaderIfNeeded() {
        if (!this.initHeader) {
            synchronized (this) {
                if (!this.initHeader) {
                    this.binaryDecoder = new BufferWarpBinaryDecoder(this.rawValue);
                    try {
                        this.deserializer.deserializeHeader(this.binaryDecoder, this);
                    } catch (IOException ex) {
                        throw new RuntimeException("capture-dstore decode header failed", ex);
                    }
                    this.initHeader = true;
                }
            }
        }
    }

    protected void initPayloadIfNeeded() {
        if (!this.initPayload) {
            initHeaderIfNeeded();
            synchronized (this) {
                if (!this.initPayload) {
                    try {
                        this.deserializer.deserializePayload(this.binaryDecoder, this);
                    } catch (IOException ex) {
                        throw new RuntimeException("capture-dstore decode body failed", ex);
                    }
                    this.initPayload = true;
                    this.binaryDecoder = null;
                }
            }
        }
    }

    @Override
    public long getId() {
        initHeaderIfNeeded();
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setSourcePosition(String sourcePosition) {
        this.sourcePosition = sourcePosition;
    }

    public String getTransactionId() {
        initHeaderIfNeeded();
        return this.transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public long getTimestamp() {
        initHeaderIfNeeded();
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public long getSourceTimestamp() {
        initHeaderIfNeeded();
        return this.timestamp;
    }

    @Override
    public OperationType getOperationType() {
        initHeaderIfNeeded();
        return this.operationType;
    }

    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    @Override
    public LazyRecordSchema getSchema() {
        return this.getSchema(true);
    }

    public LazyRecordSchema getSchema(boolean load) {
        if (load) {
            initHeaderIfNeeded();
            if (null == this.recordSchema) {
                initPayloadIfNeeded();
            }
        }
        if (recordSchema == null) {
            return new LazyRecordSchema(this, null, null, null, null, null, 0);
        }
        return this.recordSchema;
    }

    public void setRecordSchema(LazyRecordSchema recordSchema) {
        this.recordSchema = recordSchema;
    }

    public void setBeforeImage(RowImage beforeImage) {
        this.beforeImage = beforeImage;
    }

    @Override
    public RowImage getBeforeImage() {
        initPayloadIfNeeded();
        return this.beforeImage;
    }

    public void setAfterImage(RowImage afterImage) {
        this.afterImage = afterImage;
    }

    @Override
    public RowImage getAfterImage() {
        initPayloadIfNeeded();
        return this.afterImage;
    }

    public void setExtendedProperty(Map<String, String> extendedProperty) {
        this.extendedProperty = extendedProperty;
    }

    @Override
    public Map<String, String> getExtendedProperty() {
        initHeaderIfNeeded();
        return this.extendedProperty;
    }

    /**
     * This function is used to truncated the fully id to lower 31 bits. The purpose of it is to make writer2.0 happy.
     * When all writer2.0 are replaced, we can use getId() directly.
     */
    private int getIdLowerValue() {
        final int positiveIntMask = 0x7FFFFFFF;
        return (int) (getId() & positiveIntMask);
    }

    public void setSourceSafePosition(String sourceSafePosition) {
        this.sourceSafePosition = sourceSafePosition;
    }

    public long offset() {
        return offset;
    }

    public int getSourceTypeCode() {
        return sourceTypeCode;
    }

    public void setSourceTypeCode(int sourceTypeCode) {
        this.sourceTypeCode = sourceTypeCode;
    }

    public void setSourceTypeAndVersion(Pair<String, String> sourceTypeAndVersion) {
        this.sourceTypeAndVersion = sourceTypeAndVersion;
    }

    public long getBornTimestamp() {
        initHeaderIfNeeded();
        if (0 == bornTimestamp) {
            if (OperationType.INSERT == this.operationType
                    || OperationType.UPDATE == this.operationType
                    || OperationType.DELETE == this.operationType
                    || OperationType.DDL == this.operationType) {
                return TimeUtils.getTimestampSeconds(getTimestamp());
            }
            initPayloadIfNeeded();
        }
        return bornTimestamp > 0 ? bornTimestamp : TimeUtils.getTimestampSeconds(getTimestamp());
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public void commit(String metadata) {
        userCommitCallBack.commit(topicPartition, getSourceTimestamp(), offset, metadata);
    }
}
