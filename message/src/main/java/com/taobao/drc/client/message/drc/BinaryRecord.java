package com.taobao.drc.client.message.drc;

import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.message.ByteString;
import com.taobao.drc.client.message.DataMessage.Record;
import com.taobao.drc.client.message.FieldParseListener;
import com.taobao.drc.client.utils.BinaryMessageUtils;
import com.taobao.drc.client.protocol.binary.DataType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.taobao.drc.client.message.DataMessage.Record.Field.Type.*;
import static com.taobao.drc.client.enums.DBType.*;

import static com.taobao.drc.client.protocol.binary.DataType.DT_MASK;
import static com.taobao.drc.client.protocol.binary.DataType.DT_UINT8;
import static com.taobao.drc.client.utils.MessageConstant.DEFAULT_CHARSET;
import static com.taobao.drc.client.utils.MessageConstant.DEFAULT_FIELD_ENCODING;
import static com.taobao.drc.client.utils.MessageConstant.DEFAULT_ENCODING;

/**
 * Created by jianjundeng on 7/18/16.
 */
public class BinaryRecord extends Record {
    private static final Logger log = LoggerFactory.getLogger(BinaryRecord.class);

    //old version header length
    private static final int OLD_VERSION_2_HEADER_LEN = 88;

    //new version header length
    private static final int NEW_VERSION_2_HEADER_LEN = 96;

    private static final int VERSION_3_HEADER_LEN = 104;

    private static final int PREFIX_LENGTH = 12;

    private static final long MAX_LONG = 0xFFFFFFFFFFFFFFFFL;

    private int brVersion = (byte) 0xff;

    private int srcType = (byte) 0xff;

    private int op = (byte) 0xff;

    private int lastInLogEvent = (byte) 0xff;

    private long srcCategory = MAX_LONG;

    private long id = MAX_LONG;

    private long timestamp = MAX_LONG;

    private long encoding = MAX_LONG;

    private long instanceOffset = MAX_LONG;

    private long dbNameOffset = MAX_LONG;

    private long tbNameOffset = MAX_LONG;

    private long colNamesOffset = MAX_LONG;

    private long colTypesOffset = MAX_LONG;

    private long fileNameOffset = MAX_LONG;

    private long fileOffset = MAX_LONG;

    private long oldColsOffset = MAX_LONG;

    private long newColsOffset = MAX_LONG;

    private long pkKeysOffset = MAX_LONG;

    private long ukColsOffset = MAX_LONG;

    private long colsEncodingOffset = MAX_LONG;

    private long filterRuleValOffset = MAX_LONG;

    private long tailOffset = MAX_LONG;

    private long keysOffset = MAX_LONG;

    /**
     * buf parse data
     */
    private String dbName;

    private String tableName;

    private String serverId;

    private ByteBuf byteBuf;

    private Set<String> keysValue;

    private Set<String> pkValues;

    private Long encodingNum;

    /**
     * type bitmap,get array type bytes by type index
     * array first byte : 1,  array element is unsigned byte
     * 2,  array element is unsigned short
     * 4,  array element is unsigned int
     * 8,  array element is long
     */
    private final static int[] elementArray = {0, 1, 1, 2, 2, 4, 4, 8, 8};

    private final static int BYTE_SIZE = 1;

    private final static int INT_SIZE = Integer.SIZE / Byte.SIZE;

    @Override
    public Type getOpt() {
        return Type.valueOf(op);
    }

    @Override
    public String getId() {
        return Long.toString(id);
    }

    @Override
    public String getDbname() {
        if (dbName == null) {
            if ((int) dbNameOffset >= 0) {
                dbName = BinaryMessageUtils.getString(byteBuf.array(), (int) dbNameOffset, DEFAULT_CHARSET);
            }
        }
        return dbName;
    }

    @Override
    public String getTablename() {
        if (tableName == null) {
            if ((int) tbNameOffset >= 0) {
                tableName = BinaryMessageUtils.getString(byteBuf.array(), (int) tbNameOffset, DEFAULT_CHARSET);
            }
        }
        return tableName;
    }

    @Override
    public String getCheckpoint() {
        return fileOffset + "@" + fileNameOffset;
    }

    @Override
    public String getTimestamp() {
        return Long.toString(timestamp);
    }

    @Override
    public String getServerId() {
        if (serverId == null) {
            if ((int) instanceOffset >= 0) {
                serverId = BinaryMessageUtils.getString(byteBuf.array(), (int) instanceOffset, DEFAULT_CHARSET);
            }
        }
        return serverId;
    }

    @Override
    public String getPrimaryKeys() {
        try {
            if ((int) pkKeysOffset < 0) {
                return "";
            } else {
                List<Integer> pks = BinaryMessageUtils.getArray(byteBuf.array(),(int) pkKeysOffset);
                List<ByteString> names = BinaryMessageUtils.getByteStringList(byteBuf.array(),colNamesOffset);
                StringBuilder pkKeyName = new StringBuilder();
                if (pks != null) {
                    for (int idx : pks) {
                        if (pkKeyName.length() != 0)
                            pkKeyName.append(",");
                        pkKeyName.append(names.get(idx).toString(DEFAULT_FIELD_ENCODING));
                    }
                }
                return pkKeyName.toString();
            }
        } catch (Exception e) {
            log.error("get primary keys error",e);
        }
        return "";
    }

    @Override
    public DBType getDbType() {
        switch (srcType) {
            case 0:
                return MYSQL;
            case 1:
                return OCEANBASE;
            case 2:
                return HBASE;
            case 3:
                return ORACLE;
            case 4:
                return OCEANBASE1;
            default:
                return DBType.UNKNOWN;
        }
    }

    @Override
    public boolean isQueryBack() {
        switch ((int) srcCategory) {
            case 1:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean isFirstInLogevent() {
        return lastInLogEvent == 1 ? true : false;
    }

    @Override
    public List<Field> getFieldList() {
        try {
            if (fields == null) {
                if (colNamesOffset < 0 || colTypesOffset < 0 || oldColsOffset < 0 || newColsOffset < 0) {
                    return new ArrayList<Field>();
                }
                //global encoding
                ByteBuf wrapByteBuf = Unpooled.wrappedBuffer(byteBuf.array()).order(ByteOrder.LITTLE_ENDIAN);
                //global encoding
                wrapByteBuf.readerIndex(PREFIX_LENGTH + (int)encoding+1);
                int length = (int) wrapByteBuf.readUnsignedInt();
                String encodingStr=new String(wrapByteBuf.array(), PREFIX_LENGTH + 5 + (int)encoding, length - 1, DEFAULT_ENCODING);
                //pk info
                List<Integer> pks = null;
                if ((int) pkKeysOffset > 0) {
                    pks = BinaryMessageUtils.getArray(wrapByteBuf.array(), (int) pkKeysOffset);
                }
                //get column count
                wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + colNamesOffset + BYTE_SIZE));
                int count = wrapByteBuf.readInt();
                fields = new ArrayList<Field>(count);
                //op type array
                wrapByteBuf.readerIndex(PREFIX_LENGTH + (int) colTypesOffset);
                byte t = wrapByteBuf.readByte();
                int elementSize = elementArray[t & DataType.DT_MASK];
                //encoding
                int colEncodingCount = 0;
                int currentEncodingOffset = 0;
                if (colsEncodingOffset != -1) {
                    wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + colsEncodingOffset + BYTE_SIZE));
                    colEncodingCount = wrapByteBuf.readInt();
                    currentEncodingOffset = (int) wrapByteBuf.readUnsignedInt();
                }
                //column name
                wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + colNamesOffset + BYTE_SIZE + INT_SIZE));
                int currentColNameOffset = (int) wrapByteBuf.readUnsignedInt();
                //old col value
                wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + oldColsOffset + BYTE_SIZE));
                int oldColCount = wrapByteBuf.readInt();
                int currentOldColOffset = -1;
                if (0 != oldColCount) {
                    currentOldColOffset = (int) wrapByteBuf.readUnsignedInt();
                }
                //new col value
                wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + newColsOffset + BYTE_SIZE));
                int newColCount = wrapByteBuf.readInt();
                int currentNewColOffset = -1;
                if (0 != newColCount) {
                    currentNewColOffset = (int) wrapByteBuf.readUnsignedInt();
                }
                //start loop
                for (int i = 0; i < count; i++) {
                    //get pk boolean
                    boolean isPk = false;
                    if (pks != null && pks.contains(i)) {
                        isPk = true;
                    }
                    //get real op type
                    int type = 0;
                    wrapByteBuf.readerIndex(PREFIX_LENGTH + (int) colTypesOffset + BYTE_SIZE + INT_SIZE + i * elementSize);
                    switch (elementSize) {
                        case 1:
                            type = wrapByteBuf.readUnsignedByte();
                            break;
                        case 2:
                            type = wrapByteBuf.readUnsignedShort();
                            break;
                        case 4:
                            type = (int) wrapByteBuf.readUnsignedInt();
                            break;
                        case 8:
                            type = (int) wrapByteBuf.readLong();
                            break;
                    }
                    //get real encoding
                    String realEncoding = encodingStr;
                    wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + colsEncodingOffset+BYTE_SIZE+INT_SIZE+(i+1)*INT_SIZE));
                    if (colEncodingCount >0) {
                        int nextEncodingOffset= (int) wrapByteBuf.readUnsignedInt();
                        ByteString encodingByteString=new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                                currentEncodingOffset + BYTE_SIZE+INT_SIZE+(count+1)*INT_SIZE + (int) colsEncodingOffset, nextEncodingOffset - currentEncodingOffset - 1);
                        realEncoding = encodingByteString.toString();
                        if (realEncoding.isEmpty()) {
                            if ((type == 253 || type == 254) && Field.MYSQL_TYPES[type] == Field.Type.STRING) {
                                realEncoding = "binary";
                            } else if (Field.MYSQL_TYPES[type] == Field.Type.BLOB) {
                                realEncoding = "";
                            } else if (type == 245) {
                                realEncoding= "utf8mb4";
                            } else {
                                realEncoding = DEFAULT_FIELD_ENCODING;
                            }
                        }
                        currentEncodingOffset=nextEncodingOffset;
                    }
                    //type change from blob to string if encoding is not empty
                    //Bug: This line should like following : Field.MYSQL_TYPES[type] == Field.Type.BLOB
                    //But the active of java2object will changed for text, medium text and long text
                    //And consumers using hard cord and treat source column  have text type as byte array
                    //which should be string actually.
                    if (!realEncoding.isEmpty() && type == BLOB.ordinal()) {
                        type = 15;
                    }
                    //colName
                    wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + colNamesOffset + BYTE_SIZE + INT_SIZE + (i + 1) * INT_SIZE));
                    int nextColNameOffset = (int) wrapByteBuf.readUnsignedInt();
                    ByteString ColNameByteString = new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                            currentColNameOffset + BYTE_SIZE + INT_SIZE + (count + 1) * INT_SIZE + (int) colNamesOffset, nextColNameOffset - currentColNameOffset - 1);
                    String columnName=ColNameByteString.toString();
                    currentColNameOffset = nextColNameOffset;
                    //old col
                    if (oldColCount != 0) {
                        wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + oldColsOffset + BYTE_SIZE + INT_SIZE + (i + 1) * INT_SIZE));
                        int nextOldColOffset = (int) wrapByteBuf.readUnsignedInt();
                        ByteString value = null;
                        if (nextOldColOffset != currentOldColOffset) {
                            value = new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                                    currentOldColOffset + BYTE_SIZE + INT_SIZE + (count + 1) * INT_SIZE + (int) oldColsOffset, nextOldColOffset - currentOldColOffset - 1);
                        }
                        fields.add(new Field(columnName, type, realEncoding, value, isPk));
                        currentOldColOffset = nextOldColOffset;
                    }
                    //new col
                    if (newColCount != 0) {
                        wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + newColsOffset + BYTE_SIZE + INT_SIZE + (i + 1) * INT_SIZE));
                        int nextNewColOffset = (int) wrapByteBuf.readUnsignedInt();
                        ByteString value = null;
                        if (currentNewColOffset != nextNewColOffset) {
                            value = new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                                    currentNewColOffset + BYTE_SIZE + INT_SIZE + (count + 1) * INT_SIZE + (int) newColsOffset, nextNewColOffset - currentNewColOffset - 1);
                        }
                        fields.add(new Field(columnName, type, realEncoding, value, isPk));
                        currentNewColOffset = nextNewColOffset;
                    }
                }
            }

        } catch (Exception e) {
            log.error("", e);
        }
        return fields;
    }

    /**
     * buffer offset
     *
     * @param byteBuf
     * @throws Exception
     */
    public void setByteBuf(ByteBuf byteBuf) throws Exception {
        this.byteBuf = byteBuf;
        //omit first 12 bytes
        byteBuf.readerIndex(PREFIX_LENGTH);
        if ((byteBuf.readByte() & DT_MASK) != DT_UINT8) {
            throw new Exception("parse error");
        }
        long count = byteBuf.readInt();
        boolean old = false;
        switch ((int) count) {
            case OLD_VERSION_2_HEADER_LEN:
                old = true;
                break;
            case VERSION_3_HEADER_LEN:
            case NEW_VERSION_2_HEADER_LEN:
                break;
            default:
                throw new Exception("");
        }
        brVersion = byteBuf.readUnsignedByte();
        srcType = byteBuf.readUnsignedByte();
        op = byteBuf.readUnsignedByte();
        lastInLogEvent = byteBuf.readByte();
        srcCategory = byteBuf.readInt();
        id = byteBuf.readLong();
        timestamp = byteBuf.readLong();
        encoding = byteBuf.readInt();
        instanceOffset = byteBuf.readInt();
        //omit timeMark
        byteBuf.readInt();
        dbNameOffset = byteBuf.readInt();
        tbNameOffset = byteBuf.readInt();
        colNamesOffset = byteBuf.readInt();
        colTypesOffset = byteBuf.readInt();

        //process new version
        if (!old) {
            keysOffset = byteBuf.readInt();
            fileNameOffset = byteBuf.readLong();
            fileOffset = byteBuf.readLong();
            oldColsOffset = byteBuf.readInt();
            newColsOffset = byteBuf.readInt();
        } else {
            //process old version
            fileNameOffset = byteBuf.readInt();
            fileOffset = byteBuf.readInt();
            oldColsOffset = byteBuf.readInt();
            newColsOffset = byteBuf.readInt();
            keysOffset = byteBuf.readInt();
        }

        pkKeysOffset = byteBuf.readInt();
        ukColsOffset = byteBuf.readInt();

        if (brVersion > 1)
            colsEncodingOffset = byteBuf.readLong();
        //thread id& trace id
        if (brVersion == 3) {
            filterRuleValOffset = byteBuf.readInt();
            tailOffset = byteBuf.readInt();
        }
    }


    public String getTraceId() throws Exception {
        List<ByteString> list = BinaryMessageUtils.getByteStringList(byteBuf.array(), filterRuleValOffset);
        if (list == null || list.size() == 0 || list.size() < 3) {
            return null;
        }
        ByteString traceId = list.get(2);
        return traceId == null ? null : traceId.toString();
    }

    public String getThreadId() throws Exception {
        long threadId = 0l;
        List<Integer> list = BinaryMessageUtils.getArray(byteBuf.array(), (int) tailOffset);
        if (list == null || list.size() == 0) {
            return null;
        }
        threadId += (long) list.get(0);
        threadId += ((long) list.get(1)) << 8;
        threadId += ((long) list.get(2)) << 16;
        threadId += ((long) list.get(3)) << 24;
        return String.valueOf(threadId);
    }

    /**
     * only use in oracle rt mode
     * @return
     * @throws Exception
     */
    @Override
    public Long getLogSeqNum() throws Exception {
        if (encodingNum == null) {
            String encodeStr = BinaryMessageUtils.getString(byteBuf.array(), (int) encoding, DEFAULT_CHARSET);
            try {
                encodingNum = Long.valueOf(encodeStr);
            } catch (Exception e) {
                encodingNum = 0l;
            }
            return encodingNum;
        } else {
            return encodingNum;
        }
    }

    /**
     * get Pk value
     *
     * @return
     */
    @Override
    public Set<String> getPkValue() {
        try {
            if (pkValues != null) {
                return pkValues;
            }
            if (colNamesOffset < 0 || colTypesOffset < 0 || oldColsOffset < 0 || newColsOffset < 0) {
                return null;
            }

            //get key str
            pkValues = new HashSet<String>();
            List<Integer> indexList = BinaryMessageUtils.getArray(byteBuf.array(), (int) pkKeysOffset);
            if (indexList == null || indexList.size() == 0) {
                return null;
            }

            //get value offset by op type
            switch (getOpt()) {
                case INSERT:
                case REPLACE:
                case INDEX_INSERT:
                case INDEX_REPLACE:
                    getPkKeys((int) newColsOffset, indexList);
                    break;
                case DELETE:
                case INDEX_DELETE:
                    getPkKeys((int) oldColsOffset, indexList);
                    break;
                case UPDATE:
                case INDEX_UPDATE:
                    switch (getDbType()) {
                        case ORACLE:
                        case MYSQL:
                        case OCEANBASE1:
                            getPkKeys((int) oldColsOffset, indexList);
                            getPkKeys((int) newColsOffset, indexList);
                            break;
                        case OCEANBASE:
                            getPkKeys((int) newColsOffset, indexList);
                            break;
                    }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return pkValues;
    }


    /**
     * get pk uk value,combine
     *
     * @return
     */
    public Set<String> getKeysValue() throws Exception {
        try {
            if (keysValue != null) {
                return keysValue;
            }
            if (colNamesOffset < 0 || colTypesOffset < 0 || oldColsOffset < 0 || newColsOffset < 0) {
                return null;
            }

            //get key str
            keysValue = new HashSet<String>();
            List<ByteString> keys = BinaryMessageUtils.getByteStringList(byteBuf.array(), (int) keysOffset);
            if (keys == null || keys.size() == 0) {
                return null;
            }
            //get value offset by op type
            switch (getOpt()) {
                case INSERT:
                case REPLACE:
                case INDEX_INSERT:
                case INDEX_REPLACE:
                    getKeys((int) newColsOffset, keys);
                    break;
                case DELETE:
                case INDEX_DELETE:
                    getKeys((int) oldColsOffset, keys);
                    break;
                case UPDATE:
                case INDEX_UPDATE:
                    switch (getDbType()) {
                        case ORACLE:
                        case MYSQL:
                        case OCEANBASE1:
                            getKeys((int) oldColsOffset, keys);
                            getKeys((int) newColsOffset, keys);
                            break;
                    }
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return keysValue;
    }

    private void getPkKeys(int valueOffset, List<Integer> keys) {
        if (valueOffset == -1) {
            return;
        }
        ByteBuf wrapByteBuf = Unpooled.wrappedBuffer(byteBuf.array()).order(ByteOrder.LITTLE_ENDIAN);
        //get field count
        wrapByteBuf.readerIndex(PREFIX_LENGTH + valueOffset + 1);
        int fieldCount = wrapByteBuf.readInt();
        StringBuilder sb = new StringBuilder();
        for (int index : keys) {
            wrapByteBuf.readerIndex(PREFIX_LENGTH + valueOffset + 5 + index * 4);
            int start = (int) wrapByteBuf.readUnsignedInt();
            int end = (int) wrapByteBuf.readUnsignedInt();
            //uk perhaps null
            if (end - start == 0) {
                continue;
            }
            String k = new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                    valueOffset + 5 + (fieldCount + 1) * 4 + start, end - start - 1).toString();
            sb.append(k);
        }
        if (sb.length() > 0) {
            pkValues.add(sb.toString());
        } else {
            pkValues.add(null);
        }
    }

    private void getKeys(int valueOffset, List<ByteString> keys) {
        if (valueOffset == -1) {
            return;
        }
        ByteBuf wrapByteBuf = Unpooled.wrappedBuffer(byteBuf.array()).order(ByteOrder.LITTLE_ENDIAN);

        //get field count
        wrapByteBuf.readerIndex(PREFIX_LENGTH + valueOffset + 1);
        int fieldCount = wrapByteBuf.readInt();

        //parse
        for (ByteString key : keys) {
            String keyStr = key.toString();
            int m = 0;
            while (true) {
                int i = keyStr.indexOf("(", m);
                if (i == -1) {
                    break;
                }
                int j = keyStr.indexOf(")", i);
                if (j == -1) {
                    log.error("parse key error");
                    return;
                }
                m = j;
                String[] parts = keyStr.substring(i + 1, j).split(",");
                StringBuilder sb = new StringBuilder();
                for (String indexStr : parts) {
                    int index = Integer.parseInt(indexStr);
                    wrapByteBuf.readerIndex(PREFIX_LENGTH + valueOffset + 5 + index * 4);
                    int start = (int) wrapByteBuf.readUnsignedInt();
                    int end = (int) wrapByteBuf.readUnsignedInt();
                    //uk perhaps null
                    if (end - start == 0) {
                        continue;
                    }
                    String k = new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                            valueOffset + 5 + (fieldCount + 1) * 4 + start, end - start - 1).toString();
                    sb.append(k);
                }
                if (sb.length() > 0) {
                    keysValue.add(sb.toString());
                } else {
                    keysValue.add(null);
                }
            }
        }
    }

    public String getUniqueColNames() {
        try {
            if ((int) ukColsOffset < 0)
                return "";
            else {
                List<Integer> uks = BinaryMessageUtils.getArray(byteBuf.array(),(int)ukColsOffset);
                List<ByteString> names = BinaryMessageUtils.getByteStringList(byteBuf.array(),colNamesOffset);
                StringBuilder ukKeyName = new StringBuilder();
                if (uks != null) {
                    for (int idx : uks) {
                        if (ukKeyName.length() != 0)
                            ukKeyName.append(",");
                        ukKeyName.append(names.get(idx).toString(DEFAULT_ENCODING));
                    }
                }
                return ukKeyName.toString();
            }
        } catch (Exception e) {
            log.error("get uk col name error",e);
            return "";
        }
    }

    public long getCRCValue()throws Exception{
        long crcValue=0l;
        if(tailOffset == -1) {
            return 0l;
        }
        List<Integer> list= BinaryMessageUtils.getArray(byteBuf.array(), (int) tailOffset);
        if(list==null||list.size()!=12){
            return 0l;
        }
        crcValue+=(long)list.get(8);
        crcValue+=((long)list.get(9))<<8;
        crcValue+=((long)list.get(10))<<16;
        crcValue+=((long)list.get(11))<<24;
        return crcValue;
    }

    public byte[] getByteArray()throws Exception{
        return byteBuf.array();
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("RecordType: ").append(getOpt()).append("\n")
                .append("DBName: ").append(getDbname()).append("\n")
                .append("TableName: ").append(getTablename()).append("\n")
                .append("PrimaryKey: ").append(getPrimaryKeys()).append("\n");
        try {
            stringBuilder.append("ThreadID: ").append(getThreadId()).append("\n")
                    .append("TraceID: ").append(getTraceId()).append("\n");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }

        try {
            stringBuilder.append("RegionId: ").append(getRegionId()).append("\n");
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

    @Override
    public void fieldListParse(FieldParseListener fieldParseListener) throws Exception {
        if (colNamesOffset < 0 || colTypesOffset < 0 || oldColsOffset < 0 || newColsOffset < 0) {
            return;
        }

//        if(fieldMetaInfoList!=null){
//            notifyFromMetaCache(fieldParseListener);
//            return;
//        }
        ByteBuf wrapByteBuf = Unpooled.wrappedBuffer(byteBuf.array()).order(ByteOrder.LITTLE_ENDIAN);
        //global encoding
        wrapByteBuf.readerIndex(PREFIX_LENGTH + (int)encoding+1);
        int length = (int) wrapByteBuf.readUnsignedInt();
        String encodingStr=new String(wrapByteBuf.array(), PREFIX_LENGTH + 5 + (int)encoding, length - 1, DEFAULT_ENCODING);

        //pk info
        List<Integer> pks = null;
        if ((int) pkKeysOffset > 0) {
            wrapByteBuf.readerIndex(PREFIX_LENGTH + (int) pkKeysOffset);
            byte t = wrapByteBuf.readByte();
            int count = (int) wrapByteBuf.readUnsignedInt();
            if (count > 0) {
                pks=new ArrayList<Integer>(count);
                int elementSize = elementArray[t & DataType.DT_MASK];
                for (int i = 0; i < count; i++) {
                    switch (elementSize) {
                        case 1:
                            pks.add((int) wrapByteBuf.readUnsignedByte());
                            break;
                        case 2:
                            pks.add(wrapByteBuf.readUnsignedShort());
                            break;
                        case 4:
                            pks.add((int) wrapByteBuf.readUnsignedInt());
                            break;
                        case 8:
                            pks.add((int) wrapByteBuf.readLong());
                            break;
                    }
                }
            }
        }
        //get column count
        wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + colNamesOffset + BYTE_SIZE));
        int count = wrapByteBuf.readInt();
        //encoding
        int colEncodingCount = 0;
        int currentEncodingOffset = 0;
        if (colsEncodingOffset != -1) {
            wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + colsEncodingOffset + BYTE_SIZE));
            colEncodingCount = wrapByteBuf.readInt();
            currentEncodingOffset = (int) wrapByteBuf.readUnsignedInt();
        }
        //column name
        wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + colNamesOffset + BYTE_SIZE + INT_SIZE));
        int currentColNameOffset = (int) wrapByteBuf.readUnsignedInt();
        //old col value
        wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + oldColsOffset + BYTE_SIZE));
        int oldColCount = wrapByteBuf.readInt();
        int currentOldColOffset = (int) wrapByteBuf.readUnsignedInt();
        //new col value
        wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + newColsOffset + BYTE_SIZE));
        int newColCount = wrapByteBuf.readInt();
        int currentNewColOffset = (int) wrapByteBuf.readUnsignedInt();

        //start loop
        for (int i = 0; i < count; i++) {
            //get pk boolean
            boolean isPk = false;
            if (pks != null && pks.contains(i)) {
                isPk = true;
            }
            //get real op type
            wrapByteBuf.readerIndex(PREFIX_LENGTH + (int) colTypesOffset + BYTE_SIZE + INT_SIZE + i);
            int type=wrapByteBuf.readUnsignedByte();
            //get real encoding
            String realEncoding = encodingStr;
            if (colEncodingCount > 0) {
                wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + colsEncodingOffset + BYTE_SIZE + INT_SIZE + (i + 1) * INT_SIZE));
                int nextEncodingOffset = (int) wrapByteBuf.readUnsignedInt();
                if(nextEncodingOffset-currentEncodingOffset==1){
                    realEncoding = DEFAULT_FIELD_ENCODING;
                }else {
                    ByteString encodingByteString = new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                            currentEncodingOffset + BYTE_SIZE + INT_SIZE + (count + 1) * INT_SIZE + (int) colsEncodingOffset, nextEncodingOffset - currentEncodingOffset - 1);
                    realEncoding = encodingByteString.toString();
                }
                currentEncodingOffset = nextEncodingOffset;
            }
            //type change from blob to string if encoding is not empty
            // BUG: should use Type.valueOf(type), but not fix
            if (!realEncoding.isEmpty() && type == BLOB.ordinal()) {
                type = 15;
            }
            //colName
            wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + colNamesOffset + BYTE_SIZE + INT_SIZE + (i + 1) * INT_SIZE));
            int nextColNameOffset = (int) wrapByteBuf.readUnsignedInt();
            ByteString ColNameByteString = new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                    currentColNameOffset + BYTE_SIZE + INT_SIZE + (count + 1) * INT_SIZE + (int) colNamesOffset, nextColNameOffset - currentColNameOffset - 1);
            currentColNameOffset = nextColNameOffset;
            String columnName=ColNameByteString.toString();
            Field prev = null;
            Field next = null;
            //old col
            if (oldColCount != 0) {
                wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + oldColsOffset + BYTE_SIZE + INT_SIZE + (i + 1) * INT_SIZE));
                int nextOldColOffset = (int) wrapByteBuf.readUnsignedInt();
                ByteString value = null;
                if (nextOldColOffset != currentOldColOffset) {
                    value = new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                            currentOldColOffset + BYTE_SIZE + INT_SIZE + (count + 1) * INT_SIZE + (int) oldColsOffset, nextOldColOffset - currentOldColOffset - 1);
                }
                prev = new Field(columnName, type, realEncoding, value, isPk);
                currentOldColOffset = nextOldColOffset;
            }
            //new col
            if (newColCount != 0) {
                wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + newColsOffset + BYTE_SIZE + INT_SIZE + (i + 1) * INT_SIZE));
                int nextNewColOffset = (int) wrapByteBuf.readUnsignedInt();
                ByteString value = null;
                if (currentNewColOffset != nextNewColOffset) {
                    value = new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                            currentNewColOffset + BYTE_SIZE + INT_SIZE + (count + 1) * INT_SIZE + (int) newColsOffset, nextNewColOffset - currentNewColOffset - 1);
                }
                next = new Field(columnName, type, realEncoding, value, isPk);
                currentNewColOffset = nextNewColOffset;
            }
            fieldParseListener.parseNotify(prev, next);
        }

    }

    public long getFileNameOffset() {
        return this.fileNameOffset;
    }
}
