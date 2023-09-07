package com.taobao.drc.client.message.dts;

import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.message.ByteString;
import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.message.FieldParseListener;
import com.taobao.drc.client.message.dts.record.RecordField;
import com.taobao.drc.client.message.dts.record.RecordIndexInfo;
import com.taobao.drc.client.message.dts.record.utils.BytesUtil;
import com.taobao.drc.client.message.dts.record.value.DateTime;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author xusheng.zkw
 */
public class DStoreAvroRecord extends DataMessage.Record {
    private static final Logger log = LoggerFactory.getLogger(DStoreAvroRecord.class);
    private final BinaryDecoder binaryDecoder;
    private final byte[] rawData;
    private final AtomicBoolean initiateHeader = new AtomicBoolean(false);
    private final CountDownLatch initiateLatch = new CountDownLatch(1);
    private final AtomicBoolean initiateBody = new AtomicBoolean(false);
    private final CountDownLatch initiateBodyLatch = new CountDownLatch(1);
    private DStoreRecordSchema schema;
    private long id;
    private long timestamp;
    private Type op;
    private int rawOp;
    private String checkpoint;
    private int srcType;
    private String transactionId;
    private String threadId;
    private String traceId;
    private String version;
    private DStoreRowImage beforeRowImage;
    private DStoreRowImage afterRowImage;

    public DStoreAvroRecord(byte[] data, long offset, String topic) {
        this.binaryDecoder = DecoderFactory.get().binaryDecoder(data, null);
        this.rawData = data;
        this.addAttribute("offset", offset + "");
        this.addAttribute("topic", topic + "");
        this.addAttribute("subscription", "dts");
    }

    protected void initHeader() {
        if (initiateHeader.compareAndSet(false, true)) {
            try {
                DStoreRecordDeserializer.deserializeHeader(binaryDecoder, this);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            initiateLatch.countDown();
        }
        try {
            initiateLatch.await();
        } catch (InterruptedException e) {
            log.warn("waiting for init header failed.error:[" + e.getMessage() + "]");
        }
    }

    protected void initBody() {
        initHeader();
        if (initiateBody.compareAndSet(false, true)) {
            try {
                DStoreRecordDeserializer.deserializePayload(binaryDecoder, this);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            initiateBodyLatch.countDown();
        }
        try {
            initiateBodyLatch.await();
        } catch (InterruptedException e) {
            log.warn("waiting for init body failed.error:[" + e.getMessage() + "]");
        }
    }



    @Override
    public Type getOpt() {
        initHeader();
        return op;
    }

    @Override
    public String getId() {
        initHeader();
        return Long.toString(id);
    }

    @Override
    public String getDbname() {
        initHeader();
        return null != schema ? schema.getDatabaseName() : null;
    }

    @Override
    public String getTablename() {
        initHeader();
        return null != schema ? schema.getTableName() : null;
    }

    @Override
    public String getCheckpoint() {
        initHeader();
        return checkpoint;
    }

    @Override
    public String getTimestamp() {
        initHeader();
        return Long.toString(timestamp);
    }

    /**
     * This value is the ip-port, but DStore doesn't save this value.
     * eg.127.0.0.1-3306
     * @return
     */
    @Deprecated
    @Override
    public String getServerId() {
        return StringUtils.EMPTY;
    }

    @Override
    public String getPrimaryKeys() {
        return null != schema ? getKeyNames(schema.getPrimaryIndexInfo()) : null;
    }

    @Override
    public String getUniqueColNames() {
        return null != schema ? getKeyNames(schema.getUniqueIndexInfo()) : null;
    }

    private String getKeyNames(List<RecordIndexInfo> indexInfo) {
        List<String> keys = new ArrayList<String>(indexInfo.size());
        for (RecordIndexInfo index : indexInfo) {
            String subUk = getKeyNames(index);
            if (StringUtils.isNotEmpty(subUk)) {
                keys.add(subUk);
            }
        }
        return StringUtils.join(keys, ",");
    }

    private String getKeyNames(RecordIndexInfo indexInfo) {
        if (null == indexInfo || null == indexInfo.getIndexFields() || indexInfo.getIndexFields().isEmpty()) {
            return StringUtils.EMPTY;
        }
        List<String> keys = new ArrayList<String>(indexInfo.getIndexFields().size());
        for (RecordField f : indexInfo.getIndexFields()) {
            keys.add(f.getFieldName());
        }
        return StringUtils.join(keys, ",");
    }

    @Override
    public DBType getDbType() {
        return DStoreRecordDeserializer.dbTypeDeserializers[getSrcType()];
    }

    public int getSrcType() {
        initHeader();
        return srcType;
    }

    /**
     * DStore doesn't save this value.
     * @return
     */
    @Override
    @Deprecated
    public boolean isQueryBack() {
        return false;
    }

    /**
     * for MYSQL, it returns true because the record is the LAST record in the logevent.
     * @return
     */
    @Override
    @Deprecated
    public boolean isFirstInLogevent() {
        return true;
    }

    @Override
    public List<Field> getFieldList() {
        List<RecordField> fields = null != schema ? schema.getFields() : new ArrayList<RecordField>();
        List<Field> fieldValues = new ArrayList<Field>(fields.size());
        boolean existsBefore = null != beforeRowImage;
        boolean existsAfter = null != afterRowImage;
        for (RecordField f : fields) {
            //Don't care about which OperationType Record is, just generate List<Field> when RowImage exists.
            if (existsBefore) {
                fieldValues.add(newFieldByDStoreRawValue(beforeRowImage.getValue(f), f));
            }
            if (existsAfter) {
                fieldValues.add(newFieldByDStoreRawValue(afterRowImage.getValue(f), f));
            }
        }
        return fieldValues;
    }

    private Field newFieldByDStoreRawValue(DStoreRawValue v, RecordField f) {
        ByteString byteString = null != v && null != v.getData() ? v.getData() : null;
        int type = null != f && null != f.getRawDataType() ? f.getRawDataType().getTypeId() : DStoreRecordDeserializer.MYSQL_TYPE_UNKNOWN;
        if (type == DStoreRecordDeserializer.MYSQL_TYPE_YEAR && null != byteString && byteString.getBytes().length > 0) {
            DateTime dateTime = new DateTime(byteString.toString(), DateTime.SEG_DATETIME_NAONS);
            byteString = new ByteString((dateTime.getYear() + "").getBytes());
        }
        if (type == DStoreRecordDeserializer.MYSQL_TYPE_TIMESTAMP && null != byteString && byteString.getBytes().length > 0) {
            DateTime dateTime = new DateTime(byteString.toString(), DateTime.SEG_DATETIME_NAONS);
            byte[] data = byteString.getBytes();
            try {
                long timestamp = dateTime.toUnixTimestamp();
                timestamp = (timestamp + "").length() == 13 ? timestamp / 1000 : timestamp;
                data = (timestamp + "").getBytes();
            } catch (ParseException e) {
            }
            byteString = new ByteString(data);
        }
        if (type == DStoreRecordDeserializer.MYSQL_TYPE_BLOB && null != v && StringUtils.trimToEmpty(v.getEncoding()).equalsIgnoreCase(DStoreRecordDeserializer.BINARY_ENCODE)) {
            type = DStoreRecordDeserializer.MYSQL_TYPE_STRING;
        }
        if (type == DStoreRecordDeserializer.MYSQL_TYPE_DATE && null != byteString) {
            byteString = new ByteString(new String(byteString.getBytes()).replaceAll("-", ":").getBytes());
        }
        type = type == DStoreRecordDeserializer.MYSQL_TYPE_VARCHAR ? DStoreRecordDeserializer.MYSQL_TYPE_BLOB : type;

        //ddl
        if (op == Type.DDL && f.getFieldName().equalsIgnoreCase("ddl")) {
            type = DStoreRecordDeserializer.MYSQL_TYPE_253;
        }
        String encoder = null != v ? v.getEncoding() : BytesUtil.UTF8_ENCODING;
        return new Field(f.getFieldName(), type, encoder, byteString, f.isPrimary());
    }

    @Override
    @Deprecated
    public String getTraceId() {
        initBody();
        return traceId;
    }

    @Override
    public String getThreadId() {
        initBody();
        return threadId;
    }

    /**
     * only use in oracle rt mode
     * @return
     * @throws Exception
     */
    @Override
    @Deprecated
    public Long getLogSeqNum() {
        return 0L;
    }

    @Override
    public long getCRCValue() {
        return 0;
    }

    @Override
    public byte[] getByteArray() {
        return rawData;
    }

    @Override
    @Deprecated
    public void fieldListParse(FieldParseListener fieldParseListener) {

    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("RecordType: ").append(StringUtils.trimToEmpty(getOpt().name())).append("\n")
                .append("DBName: ").append(StringUtils.trimToEmpty(getDbname())).append("\n")
                .append("TableName: ").append(StringUtils.trimToEmpty(getTablename())).append("\n")
                .append("PrimaryKey: ").append(StringUtils.trimToEmpty(getPrimaryKeys())).append("\n");

        try {
            stringBuilder.append("ThreadID: ").append(StringUtils.trimToEmpty(getThreadId())).append("\n")
                    .append("TraceID: ").append(StringUtils.trimToEmpty(getTraceId())).append("\n");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        try {
            stringBuilder.append("RegionId: ").append(getRegionId()).append("\n");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }

        //timestamp
        try {
            stringBuilder.append("Timestamp: ").append(getTimestamp()).append("\n");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }

        //offset
        try {
            stringBuilder.append("Offset: ").append(getOffset()).append("\n");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }

        List<Field> fields = getFieldList();
        if (null != fields) {
            for (Field field : fields) {
                stringBuilder.append(field.toString());
            }
        }
        return stringBuilder.toString();
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setOpt(Type op) {
        this.op = op;
    }

    public void setCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;
    }

    public void setSrcType(int srcType) {
        this.srcType = srcType;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public DStoreRecordSchema getSchema() {
        return getSchema(true);
    }

    public DStoreRecordSchema getSchema(boolean loadHeader) {
        if (loadHeader) {
            initHeader();
        }
        return schema;
    }

    public void setSchema(DStoreRecordSchema schema) {
        this.schema = schema;
    }

    public void setBeforeImage(DStoreRowImage beforeRowImage) {
        this.beforeRowImage = beforeRowImage;
    }

    public void setAfterImage(DStoreRowImage afterRowImage) {
        this.afterRowImage = afterRowImage;
    }

    public Long getOffset() {
        String offset = getAttribute("offset");
        return StringUtils.isNumeric(offset) ? Long.parseLong(offset) : null;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public int getRawOp() {
        initHeader();
        return rawOp;
    }

    public void setRawOp(int rawOp) {
        this.rawOp = rawOp;
    }
}
