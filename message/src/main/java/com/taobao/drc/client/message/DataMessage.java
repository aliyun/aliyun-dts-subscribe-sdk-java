package com.taobao.drc.client.message;

import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.enums.DBType;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

public class DataMessage{

    private List<Record> records=new ArrayList<Record>();

    public static class Record implements Serializable {

        private transient Record prev;

        private transient Record next;

        protected Type type;

        private Map<String, String> attributes=new HashMap<String, String>();

        protected List<Field> fields;

        private String safeTimestamp;

        private boolean isFirst;

        private boolean multiMode;

        private String regionId;

        private long recordLength = 0;

        private transient CheckpointManager checkpointManager;

        public void setType(Type type) {
            this.type = type;
        }

        public void addAttribute(String key, String value) {
            attributes.put(key, value);
        }

        public boolean isMultiMode() {
            return multiMode;
        }

        public void setMultiMode(boolean multiMode) {
            this.multiMode = multiMode;
        }

        public CheckpointManager getCheckpointManager() {
            return checkpointManager;
        }

        public void setCheckpointManager(CheckpointManager checkpointManager) {
            this.checkpointManager = checkpointManager;
        }

        public void ackAsConsumed(){
            if(checkpointManager != null) {
                checkpointManager.removeRecord(this);
            }
        }

        public Record getPrev() {
            return prev;
        }

        public void setPrev(Record prev) {
            this.prev = prev;
        }

        public Record getNext() {
            return next;
        }

        public void setNext(Record next) {
            this.next = next;
        }

        public String getRegionId() {
            return regionId;
        }

        public void setRegionId(String regionId) {
            this.regionId = regionId;
        }

        public long getRecordLength() {
            return  recordLength;
        }

        public void setRecordLength(long recordLength) {
            this.recordLength = recordLength;
        }

        public static class Field {

            public long length=-1;

            public boolean primaryKey;

            public String name;

            public int type=17;

            public String encoding;

            public ByteString value;

            public boolean changeValue = true;

            public static Type[] MYSQL_TYPES = new Type[256];

            static {
                MYSQL_TYPES[0] = Type.DECIMAL;
                MYSQL_TYPES[1] = Type.INT8;
                MYSQL_TYPES[2] = Type.INT16;
                MYSQL_TYPES[3] = Type.INT32;
                MYSQL_TYPES[4] = Type.FLOAT;
                MYSQL_TYPES[5] = Type.DOUBLE;
                MYSQL_TYPES[6] = Type.NULL;
                MYSQL_TYPES[7] = Type.TIMESTAMP;
                MYSQL_TYPES[8] = Type.INT64;
                MYSQL_TYPES[9] = Type.INT24;
                MYSQL_TYPES[10] = Type.DATE;
                MYSQL_TYPES[11] = Type.TIME;
                MYSQL_TYPES[12] = Type.DATETIME;
                MYSQL_TYPES[13] = Type.YEAR;
                MYSQL_TYPES[14] = Type.DATETIME;
                MYSQL_TYPES[15] = Type.STRING;
                MYSQL_TYPES[16] = Type.BIT;
                //special
                MYSQL_TYPES[245] = Type.JSON;
                MYSQL_TYPES[255] = Type.GEOMETRY;
                MYSQL_TYPES[254] = Type.STRING;
                MYSQL_TYPES[253] = Type.STRING;
                MYSQL_TYPES[252] = Type.BLOB;
                MYSQL_TYPES[251] = Type.BLOB;
                MYSQL_TYPES[250] = Type.BLOB;
                MYSQL_TYPES[249] = Type.BLOB;
                MYSQL_TYPES[248] = Type.SET;
                MYSQL_TYPES[247] = Type.ENUM;
                MYSQL_TYPES[246] = Type.DECIMAL;
            }

            public Field() {

            }

            public void setFieldName(String name) {
                this.name = name;
            }

            public void setType(int type) {
                this.type = type;
            }

            public void setValue(ByteString value) {
                this.value = value;
                this.length=value.getLen();
            }

            public enum Type {
                INT8,
                INT16,
                INT24,
                INT32,
                INT64,
                DECIMAL,
                FLOAT,
                DOUBLE,
                NULL,
                TIMESTAMP,
                DATE,
                TIME,
                DATETIME,
                YEAR,
                BIT,
                ENUM,
                SET,
                BLOB,
                GEOMETRY,
                STRING,
                JSON,
                UNKOWN
            }

            public Field(String name, int type, String encoding, ByteString value, boolean pk) {
                this.name = name;
                this.type = type;
                this.encoding = encoding;
                if (getType() == Type.STRING) {
                    if (this.encoding.isEmpty()) {
                        this.encoding = "binary";
                    }
                }
                this.value = value;
                if(this.value!=null) {
                    length = value.getLen();
                }
                primaryKey = pk;
            }

            public boolean isPrimary() {
                return primaryKey;
            }

            public void setPrimary(boolean primary) {
                primaryKey = primary;
            }

            public String getFieldname() {
                return name;
            }

            public String getEncoding() {
                if (encoding.equalsIgnoreCase("utf8mb4")) {
                    return "utf8";
                }
                return encoding;
            }

            public void setEncoding(String encoding){
                this.encoding=encoding;
            }

            public Type getType() {

                if (type > 16 && type < 245)
                    return Type.UNKOWN;
                else
                    return MYSQL_TYPES[type];

            }

            public ByteString getValue() {
                return value;
            }

            public boolean isChangeValue() {
                return changeValue;
            }

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                String separate=System.getProperty("line.separator");
                builder.append("Field name: " + name + separate);
                builder.append("Field type: " + type + separate);
                builder.append("Field length: " + length + separate);
                try {
                    if (value != null) {
                        if (encoding.equalsIgnoreCase("binary")) {
                            builder.append("Field value(binary): "
                                    + Arrays.toString(value.getBytes())
                                    + separate);
                        } else {
                            builder.append("Field value: "
                                    + value.toString(encoding)
                                    + separate);
                        }
                    } else {
                        builder.append("Field value: " + "null" +
                                separate);
                    }
                } catch (Exception e) {
                    builder.append(separate);
                }
                return builder.toString();
            }
        }

        /**
         * record type
         */
        public enum Type {
            INSERT(0),
            UPDATE(1),
            DELETE(2),
            REPLACE(3),
            HEARTBEAT(4),
            CONSISTENCY_TEST(5),
            BEGIN(6),
            COMMIT(7),
            DDL(8),
            ROLLBACK(9),
            DML(10),
            INDEX_INSERT(128),
            INDEX_UPDATE(129),
            INDEX_DELETE(130),
            INDEX_REPLACE(131),
            UNKNOWN(255);

            private int _value;

            Type(int value) {
                _value = value;
            }

            public static Type valueOf(int value) {
                switch (value){
                    case 0:
                        return INSERT;
                    case 1:
                        return UPDATE;
                    case 2:
                        return DELETE;
                    case 3:
                        return REPLACE;
                    case 4:
                        return HEARTBEAT;
                    case 5:
                        return CONSISTENCY_TEST;
                    case 6:
                        return BEGIN;
                    case 7:
                        return COMMIT;
                    case 8:
                        return DDL;
                    case 9:
                        return ROLLBACK;
                    case 10:
                        return DML;
                    case 128:
                        return INDEX_INSERT;
                    case 129:
                        return INDEX_UPDATE;
                    case 130:
                        return INDEX_DELETE;
                    case 131:
                        return INDEX_REPLACE;
                    default:
                        return UNKNOWN;
                }
            }

            public int getValue() {
                return _value;
            }
        }


        /**
         * Get the type of the record in insert, delete, update and heartbeat.
         *
         * @return the type of the record.
         */
        public Type getOpt() {
            return type;
        }


        public String getId() {
            return getAttribute("record_id");
        }

        public String getDbname() {
            return getAttribute("db");
        }

        public String getTablename() {
            return getAttribute("table_name");
        }

        public String getCheckpoint() {
            return getAttribute("checkpoint");
        }

        @Deprecated
        public String getMetadataVersion() {
            return getAttribute("meta");
        }

        public String getTimestamp() {
            return getAttribute("timestamp");
        }

        public String getSafeTimestamp(){
            return safeTimestamp;
        }

        public void setSafeTimestamp(String safeTimestamp) {
            this.safeTimestamp = safeTimestamp;
        }

        public String getServerId() {
            return getAttribute("instance");
        }

        public String getPrevId(){
            return getAttribute("prev_id");
        }

        public String getServerSeq(){
            return getAttribute("server_id");
        }

        public String getPrevServerSeq(){
            return getAttribute("prev_server_id");
        }

        public String getPrimaryKeys() {
            return getAttribute("primary");
        }

        public String getTraceInfo() {
            return "";
        }

        public String getUniqueColNames() {
            return getAttribute("unique");
        }

        public DBType getDbType() {
            String type = getAttribute("source_type");
            if (type.equalsIgnoreCase("mysql")) {
                return DBType.MYSQL;
            } else if (type.equalsIgnoreCase("oceanbase")) {
                return DBType.OCEANBASE;
            } else if (type.equalsIgnoreCase("oracle")) {
                return DBType.ORACLE;
            } else if (type.equalsIgnoreCase("hbase")) {
                return DBType.HBASE;
            } else if(type.equalsIgnoreCase("oceanbase_1_0")){
                return DBType.OCEANBASE1;
            }
            return DBType.UNKNOWN;
        }

        public boolean isQueryBack() {
            String cate = getAttribute("source_category");
            if (cate.equalsIgnoreCase("full_recorded") ||
                    cate.equalsIgnoreCase("part_recorded") ||
                    cate.equalsIgnoreCase("full_faked")) {
                return false;
            } else {
                return true;
            }
        }

        /**
         * Now the api takes on different behavior between MYSQL and OCEANBASE,
         * for MYSQL, it returns true because the record is the LAST record in
         * the logevent, while for OCEANBASE, it is true because the record is
         * the first one.
         * TBD: server for mysql need change its behavior the same as OCEANBASE
         *
         * @return
         */
        public boolean isFirstInLogevent() {
            String isFirstLogevent = getAttribute("logevent");
            if (isFirstLogevent != null && isFirstLogevent.equals("1"))
                return true;
            return false;
        }

        public String getAttribute(String key) {
            return attributes.get(key);
        }

        public int getFieldCount() {
            getFieldList();
            if(fields==null){
                return 0;
            }
            return fields.size();
        }

        /**
         * Get the field list.
         *
         * @return the field list.
         */
        public List<Field> getFieldList() {
            return fields;
        }

        public void setFieldList(List<Field> fields){
            this.fields=fields;
        }

        public String getThreadId() throws  Exception {
            return getAttribute("threadid");
        }

        public String getTraceId() throws Exception {
            return getAttribute("traceid");
        }

        public Long getLogSeqNum() throws Exception{
            throw new Exception("not support this operation");
        }

        public Set<String> getPkValue()throws Exception{
            throw new Exception("not support this operation");
        }

        public Set<String> getKeysValue() throws Exception{
            throw new Exception("not support this operation");
        }

        public void fieldListParse(FieldParseListener fieldParseListener) throws Exception{

        }

        public void setChannelFirstRecord(boolean isFirst) {
            this.isFirst = isFirst;
        }

        public boolean isChannelFirstRecord() {
            return isFirst;
        }

        public long getCRCValue()throws Exception{
            return 0;
        }

        public byte[] getByteArray() throws Exception {
            throw new Exception("not support this operation");
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            String separate=System.getProperty("line.separator");
            for (Entry<String, String> entry : attributes.entrySet()) {
                builder.append(entry.getKey() + ":" + entry.getValue());
                builder.append(separate);
            }
            builder.append(separate);
            if(fields!=null) {
                for (Field field : fields) {
                    builder.append(field.toString());
                }
                builder.append("Record length: " + recordLength + separate);
            }
            builder.append(separate);
            return builder.toString();
        }


    }

    /**
     * Get the number of all records in the message.
     *
     * @return the number of records.
     */
    public int getRecordCount() {
        return records.size();
    }

    /**
     * Get the list of records.
     *
     * @return the list of records.
     */
    public List<Record> getRecordList() {
        return records;
    }

    public void addRecord(Record record){
        records.add(record);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(super.toString());
        for (Record record : records) {
            builder.append(record.toString());
        }
        builder.append(System.getProperty("line.separator"));
        return builder.toString();
    }
}
