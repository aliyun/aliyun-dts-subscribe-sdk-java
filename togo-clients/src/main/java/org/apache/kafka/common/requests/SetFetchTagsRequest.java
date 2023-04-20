package org.apache.kafka.common.requests;

import com.taobao.drc.togo.common.businesslogic.HashTypeFilterTag;
import com.taobao.drc.togo.common.businesslogic.SpecialTypeFilterTag;
import com.taobao.drc.togo.common.businesslogic.UnitTypeFilterTag;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.kafka.common.protocol.types.Type.*;

public class SetFetchTagsRequest extends AbstractRequest {
    public static final int CONSUMER_REPLICA_ID = -1;
    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String TAG_MATCH_FUNC = "filter_match_func";


    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String FETCH_TAGS_KEY_NAME = "tags";

    // tag level field
    private static final String INCLUDE_FLAG_KEY_NAME = "include";
    private static final String FIELD_KEY_VALUES_KEY_NAME = "field_key_values";

    // tag level field names
    private static final String SCHEMA_NAME_KEY_NAME = "schema_name";
    private static final String FIELD_NAME_KEY_NAME = "field_name";
    private static final String FIELD_VALUE_KEY_NAME = "field_value";

    private final int replicaId;
    private final int tagMatchFunction;
    private final Map<TopicPartition, List<TagEntry>> fetchTags;
    private final Map<TopicPartition, HashTypeFilterTag> hashTypeFilterTags;
    private final Map<TopicPartition, SpecialTypeFilterTag> specialTypeFilterTags;
    private final Map<TopicPartition, UnitTypeFilterTag> unitTypeFilterTags;

    // api SetFetchTags
    public static final Schema SET_FETCH_TAGS_REQUEST_FIELD_KV_V0 = new Schema(
            new Field("schema_name", STRING, "The schema name of field"),
            new Field("field_name", STRING, "The field name used to tag"),
            new Field("field_value", STRING, "The tag value")
    );

    public static final Schema SET_FETCH_TAGS_REQUEST_TAG_ENTRY_V0 = new Schema(
            new Field("include", BOOLEAN, "Include the matched record or not"),
            new Field("field_key_values", new ArrayOf(SET_FETCH_TAGS_REQUEST_FIELD_KV_V0), "The field kvs used to tag")
    );

    private static final String SPECIAL_FILTER_TYPE_NAME = "special_filter_type";
    private static final String UNIT_TYPE_NAME = "unitType";
    private static final String USE_TXN_NAME = "useTxn";
    private static final String TXN_PATTERN_NAME = "txnPattern";
    private static final String USE_THREAD_ID_NAME = "useThreadID";
    private static final String USE_REGION_ID_NAME = "useRegionID";
    private static final String REGION_ID_LIST_NAME = "regionIDList";
    private static final String MAX_HASH_VALUE_NAME = "maxHashValue";
    private static final String SUB_HASH_LIST_NAME = "subHashList";
    private static final String SPECIAL_TYPE_FILTER_TAG = "special_type_filter_tag";
    private static final String UNIT_FILTER_TAG = "unit_filter_tag";
    private static final String HASH_FILTER_TAG = "hash_filter_tag";


    public static final Schema SPECIAL_TYPE_FILTER_TAG_V0 = new Schema(
            new Field(SPECIAL_FILTER_TYPE_NAME, INT64, "Indicate if begin, commit, ddl, hb, null kv is required")
    );

    public static final Schema UNIT_FILTER_TAG_V0 = new Schema(
            new Field(UNIT_TYPE_NAME, INT32, "Required unit data type"),
            new Field(USE_TXN_NAME, BOOLEAN, "If use txn table filter data"),
            new Field(TXN_PATTERN_NAME, ArrayOf.nullable(SET_FETCH_TAGS_REQUEST_TAG_ENTRY_V0), "txn pattern"),
            new Field(USE_THREAD_ID_NAME, BOOLEAN, "If use threadID filter data"),
            new Field(USE_REGION_ID_NAME, BOOLEAN, "If use RegionID filter data"),
            new Field(REGION_ID_LIST_NAME, ArrayOf.nullable(INT32), "region id list")
    );

    public static final Schema HASH_FILTER_TAG_V0 = new Schema(
            new Field(MAX_HASH_VALUE_NAME, INT32, "Max hash threshold"),
            new Field(SUB_HASH_LIST_NAME, ArrayOf.nullable(INT32), "hash list to subscribe")
    );

    public static final Schema SET_FETCH_TAGS_REQUEST_PARTITION_V0 = new Schema(
            new Field("partition", INT32, "Topic partition id."),
            new Field("tags", ArrayOf.nullable(SET_FETCH_TAGS_REQUEST_TAG_ENTRY_V0))
    );

    public static final Schema SET_FETCH_TAGS_REQUEST_PARTITION_V1 = new Schema(
            new Field("partition", INT32, "Topic partition id."),
            new Field("tags", ArrayOf.nullable(SET_FETCH_TAGS_REQUEST_TAG_ENTRY_V0)),
            new Field(SPECIAL_TYPE_FILTER_TAG, SPECIAL_TYPE_FILTER_TAG_V0, "Special filter indicate tag"),
            new Field(UNIT_FILTER_TAG, UNIT_FILTER_TAG_V0, "Unit filter tag"),
            new Field(HASH_FILTER_TAG, HASH_FILTER_TAG_V0, "Hash filter tag")
    );

    public static final Schema SET_FETCH_TAGS_REQUEST_TOPIC_V0 = new Schema(
            new Field("topic", STRING, "Topic to set tags."),
            new Field("partitions", new ArrayOf(SET_FETCH_TAGS_REQUEST_PARTITION_V0), "Partitions to set tags.")
    );

    public static final Schema SET_FETCH_TAGS_REQUEST_TOPIC_V1 = new Schema(
            new Field("topic", STRING, "Topic to set tags."),
            new Field("partitions", new ArrayOf(SET_FETCH_TAGS_REQUEST_PARTITION_V1), "Partitions to set tags.")
    );


    public static final Schema SET_FETCH_TAGS_REQUEST_V0 = new Schema(
            new Field("replica_id", INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field("topics", new ArrayOf(SET_FETCH_TAGS_REQUEST_TOPIC_V0), "Topics to fetch in the order provided.")
    );

    public static final Schema SET_FETCH_TAGS_REQUEST_V1 = new Schema(
            new Field("replica_id", INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field("topics", new ArrayOf(SET_FETCH_TAGS_REQUEST_TOPIC_V1), "Topics to fetch in the order provided.")
    );

    public static final Schema SET_FETCH_TAGS_REQUEST_V2 = new Schema(
            new Field("replica_id", INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field("topics", new ArrayOf(SET_FETCH_TAGS_REQUEST_TOPIC_V1), "Topics to fetch in the order provided."),
            new Field(TAG_MATCH_FUNC, INT32, "function to match filter tag, 0 fnmatch, 1 regex match")
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{SET_FETCH_TAGS_REQUEST_V0, SET_FETCH_TAGS_REQUEST_V1, SET_FETCH_TAGS_REQUEST_V2};
    }

    public static final class FieldKeyValueEntry {
        public final String schemaName;
        public final String fieldName;
        public final String fieldValue;

        public FieldKeyValueEntry(String schemaName, String fieldName, String fieldValue) {
            this.schemaName = schemaName;
            this.fieldName = fieldName;
            this.fieldValue = fieldValue;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type:FieldKeyValueEntry").
                    append(", schemaName=").append(schemaName).
                    append(", fieldName=").append(fieldName).
                    append(", fieldValue=").append(fieldValue).
                    append(")");
            return bld.toString();
        }
    }

    public static final class TagEntry {
        public boolean include;
        public final List<FieldKeyValueEntry> fieldKeyValues;

        public TagEntry(boolean include, List<FieldKeyValueEntry> fieldKeyValues) {
            this.include = include;
            this.fieldKeyValues = fieldKeyValues;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type:TagEntry").
                    append(", include=").append(include).
                    append(", fieldKeyValues=").append(Utils.mkString(fieldKeyValues, "|")).
                    append(")");
            return bld.toString();
        }
    }

    static final class TopicAndPartitionData<T> {
        public final String topic;
        public final Map<Integer, T> partitions;

        public TopicAndPartitionData(String topic) {
            this.topic = topic;
            this.partitions = new LinkedHashMap<>();
        }

        public static <T> List<TopicAndPartitionData<T>> batchByTopic(Map<TopicPartition, T> data) {
            List<TopicAndPartitionData<T>> topics = new ArrayList<>();
            for (Map.Entry<TopicPartition, T> topicEntry : data.entrySet()) {
                String topic = topicEntry.getKey().topic();
                int partition = topicEntry.getKey().partition();
                T partitionData = topicEntry.getValue();
                if (topics.isEmpty() || !topics.get(topics.size() - 1).topic.equals(topic))
                    topics.add(new TopicAndPartitionData<T>(topic));
                topics.get(topics.size() - 1).partitions.put(partition, partitionData);
            }
            return topics;
        }
    }

    public static class Builder extends AbstractRequest.Builder<SetFetchTagsRequest> {
        private int replicaId = CONSUMER_REPLICA_ID;
        // default is fnmatch
        private int tagMatchFunction = 0;
        private Map<TopicPartition, List<TagEntry>> fetchTags;
        private Map<TopicPartition, HashTypeFilterTag> hashTags;
        private Map<TopicPartition, SpecialTypeFilterTag> specialTags;
        private Map<TopicPartition, UnitTypeFilterTag> unitTypeTags;

        public Builder(Map<TopicPartition, List<TagEntry>> fetchTags,
                       Map<TopicPartition, HashTypeFilterTag> hashTags,
                       Map<TopicPartition, SpecialTypeFilterTag> specialTags,
                       Map<TopicPartition, UnitTypeFilterTag> unitTypeTags) {
            super(ApiKeys.SET_FETCH_TAGS);
            this.fetchTags = fetchTags;
            this.hashTags = hashTags;
            this.specialTags = specialTags;
            this.unitTypeTags = unitTypeTags;
        }

        public Builder setReplicaId(int replicaId) {
            this.replicaId = replicaId;
            return this;
        }

        public Builder setTagMatchFunction(int function) {
            this.tagMatchFunction = function;
            return this;
        }

        public Map<TopicPartition, List<TagEntry>> fetchTags() {
            return this.fetchTags;
        }

        @Override
        public SetFetchTagsRequest build(short version) {
            return new SetFetchTagsRequest(replicaId, tagMatchFunction, fetchTags, hashTags, specialTags, unitTypeTags, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type:FetchRequest").
                    append(", replicaId=").append(replicaId).
                    append(", fetchTags=").append(Utils.mkString(fetchTags, "[", "]", "=", ",")).
                    append(")");
            return bld.toString();
        }
    }

    private SetFetchTagsRequest(int replicaId, int tagMatchFunction, Map<TopicPartition, List<TagEntry>> fetchTags,
                                Map<TopicPartition, HashTypeFilterTag> hashTypeFilterTags,
                                Map<TopicPartition, SpecialTypeFilterTag> specialTypeFilterTags,
                                Map<TopicPartition, UnitTypeFilterTag> unitTypeFilterTags,
                                short version) {
        super(version);
        this.replicaId = replicaId;
        this.tagMatchFunction = tagMatchFunction;
        this.fetchTags = fetchTags;
        this.hashTypeFilterTags = hashTypeFilterTags;
        this.specialTypeFilterTags = specialTypeFilterTags;
        this.unitTypeFilterTags = unitTypeFilterTags;
    }

    private TagEntry buildTagEntryFromStruct(Struct tagStruct) {
        boolean include = tagStruct.getBoolean(INCLUDE_FLAG_KEY_NAME);
        List<FieldKeyValueEntry> kvs = new ArrayList<>();
        for (Object kvObject : tagStruct.getArray(FIELD_KEY_VALUES_KEY_NAME)) {
            Struct kvStruct = (Struct) kvObject;
            FieldKeyValueEntry kv = new FieldKeyValueEntry(
                    kvStruct.getString(SCHEMA_NAME_KEY_NAME),
                    kvStruct.getString(FIELD_NAME_KEY_NAME),
                    kvStruct.getString(FIELD_VALUE_KEY_NAME));
            kvs.add(kv);
        }
        TagEntry tagEntry = new TagEntry(include, kvs);
        return tagEntry;
    }

    public SetFetchTagsRequest(Struct struct, short versionId) {
        super(versionId);
        replicaId = struct.getInt(REPLICA_ID_KEY_NAME);
        if (versionId > 1) {
            tagMatchFunction = struct.getInt(TAG_MATCH_FUNC);
        } else {
            tagMatchFunction = 0;
        }
        fetchTags = new LinkedHashMap<>();
        hashTypeFilterTags = new LinkedHashMap<>();
        specialTypeFilterTags = new LinkedHashMap<>();
        unitTypeFilterTags = new LinkedHashMap<>();
        for (Object topicResponseObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                List<TagEntry> partitionTag = new ArrayList<>();
                Object[] fetchTagArray = partitionResponse.getArray(FETCH_TAGS_KEY_NAME);
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                // handle fetch tag
                if (null != fetchTagArray) {
                    for (Object tagObject : fetchTagArray) {
                        Struct tagStruct = (Struct) tagObject;
                        TagEntry tagEntry = buildTagEntryFromStruct(tagStruct);
                        partitionTag.add(tagEntry);
                    }
                    fetchTags.put(topicPartition, partitionTag);
                }
                if (versionId > 0) {
                    SpecialTypeFilterTag specialTypeFilterTag = buildSpecialTypeFilterTagFromStruct(partitionResponse);
                    HashTypeFilterTag hashTypeFilterTag = buildHashFilterTagFromStruct(partitionResponse);
                    UnitTypeFilterTag unitTypeFilterTag = buildUnitTypeFilterTagFromStruct(partitionResponse);
                    specialTypeFilterTags.put(topicPartition, specialTypeFilterTag);
                    hashTypeFilterTags.put(topicPartition, hashTypeFilterTag);
                    unitTypeFilterTags.put(topicPartition, unitTypeFilterTag);
                }
            }
        }
    }

    private SpecialTypeFilterTag buildSpecialTypeFilterTagFromStruct(Struct struct) {
        Struct specialTypeFilter = (Struct)struct.get(SPECIAL_TYPE_FILTER_TAG);
        long specialFilterFlag = specialTypeFilter.getLong(SPECIAL_FILTER_TYPE_NAME);
        return new SpecialTypeFilterTag(specialFilterFlag);
    }

    private void buildStructFromSpecialTypeFilterTag(Struct parent, SpecialTypeFilterTag specialTypeFilterTag) {
        Struct ret = parent.instance(SPECIAL_TYPE_FILTER_TAG);
        ret.set(SPECIAL_FILTER_TYPE_NAME, specialTypeFilterTag.getFilterTag());
        parent.set(SPECIAL_TYPE_FILTER_TAG, ret);
    }

    private HashTypeFilterTag buildHashFilterTagFromStruct(Struct struct) {
        Struct hashFilterTagStruct = (Struct)struct.get(HASH_FILTER_TAG);
        int maxHashValue = hashFilterTagStruct.getInt(MAX_HASH_VALUE_NAME);
        Object[] subHashArray = hashFilterTagStruct.getArray(SUB_HASH_LIST_NAME);
        HashTypeFilterTag ret =  new HashTypeFilterTag(maxHashValue);
        if (null != subHashArray) {
            for (Object subHash : subHashArray) {
                ret.addHashElement((Integer) subHash);
            }
        }
        return ret;
    }

    private void buildStructFromHashFilterTag(Struct parent, HashTypeFilterTag hashTypeFilterTag) {
        Struct ret = parent.instance(HASH_FILTER_TAG);
        ret.set(MAX_HASH_VALUE_NAME, hashTypeFilterTag.getMaxHashModValue());
        List<Integer> subHashList = hashTypeFilterTag.getSubHashList();
        if (null != subHashList) {
            ret.set(SUB_HASH_LIST_NAME, subHashList.toArray());
        }
        parent.set(HASH_FILTER_TAG, ret);
    }

    private UnitTypeFilterTag buildUnitTypeFilterTagFromStruct(Struct struct) {
        Struct unitTypeFilterTagStruct = (Struct)struct.get(UNIT_FILTER_TAG);
        Integer unitTypeID = unitTypeFilterTagStruct.getInt(UNIT_TYPE_NAME);
        Boolean useTxn = unitTypeFilterTagStruct.getBoolean(USE_TXN_NAME);
        Object[] txnPattern = unitTypeFilterTagStruct.getArray(TXN_PATTERN_NAME);
        Boolean useThreadID = unitTypeFilterTagStruct.getBoolean(USE_THREAD_ID_NAME);
        Boolean useRegionID = unitTypeFilterTagStruct.getBoolean(USE_REGION_ID_NAME);
        Object[] regionIDArray = unitTypeFilterTagStruct.getArray(REGION_ID_LIST_NAME);
        List<Integer> regionIDList = null;
        List<TagEntry> tagEntries = new LinkedList<>();
        if (null != txnPattern) {
            for (Object tagObject : txnPattern) {
                Struct tagStruct = (Struct) tagObject;
                TagEntry tagEntry = buildTagEntryFromStruct(tagStruct);
                tagEntries.add(tagEntry);
            }
        }
        if (null != regionIDArray) {
            regionIDList = new LinkedList<>();
            for (Object regionID : regionIDArray) {
                regionIDList.add((Integer)regionID);
            }
        }
        return UnitTypeFilterTag.fromParam(UnitTypeFilterTag.idToUnitType(unitTypeID),
                useTxn, tagEntries,
                useThreadID,
                useRegionID, regionIDList);
    }

    private void buildStructFromUnitTypeFilterTag(Struct parent, UnitTypeFilterTag unitTypeFilterTag) {
        Struct ret = parent.instance(UNIT_FILTER_TAG);
        ret.set(UNIT_TYPE_NAME, unitTypeFilterTag.getCurrentUnitType().getUnitType());
        ret.set(USE_TXN_NAME, unitTypeFilterTag.isUseTxn());

        List<TagEntry> tagEntries = unitTypeFilterTag.getTxnPattern();
        if (null == tagEntries || tagEntries.isEmpty()) {
            ret.set(TXN_PATTERN_NAME, null);
        } else {
            List<Struct> tagStructs = new ArrayList<>();
            tagEntries.forEach(tagEntry -> {
                    Struct tags = ret.instance(TXN_PATTERN_NAME);
                    buildTagListStruct(tagEntry, tags);
                    tagStructs.add(tags);
                });

            ret.set(TXN_PATTERN_NAME, tagStructs.toArray());
        }
        ret.set(USE_THREAD_ID_NAME, unitTypeFilterTag.isUseThreadID());
        ret.set(USE_REGION_ID_NAME, unitTypeFilterTag.isUseRegionID());
        List<Integer> regionIDs = unitTypeFilterTag.getRequiredRegionIDs();
        if (null != regionIDs) {
            ret.set(REGION_ID_LIST_NAME, regionIDs.toArray());
        }
        parent.set(UNIT_FILTER_TAG, ret);
    }

    private void buildTagListStruct(TagEntry tagEntry, Struct parentStruct) {
        parentStruct.set(INCLUDE_FLAG_KEY_NAME, tagEntry.include);
        List<Struct> kvStructs = new ArrayList<>();
        tagEntry.fieldKeyValues.forEach(kv -> {
            Struct kvStruct = parentStruct.instance(FIELD_KEY_VALUES_KEY_NAME);
            kvStruct.set(SCHEMA_NAME_KEY_NAME, kv.schemaName);
            kvStruct.set(FIELD_NAME_KEY_NAME, kv.fieldName);
            kvStruct.set(FIELD_VALUE_KEY_NAME, kv.fieldValue);
            kvStructs.add(kvStruct);
        });
        parentStruct.set(FIELD_KEY_VALUES_KEY_NAME, kvStructs.toArray());
    }

    @Override
    public Struct toStruct() {
        Struct struct = new Struct(ApiKeys.SET_FETCH_TAGS.requestSchema(version()));
        List<TopicAndPartitionData<List<TagEntry>>> topicsData = TopicAndPartitionData.batchByTopic(fetchTags);

        struct.set(REPLICA_ID_KEY_NAME, replicaId);
        if (version() > 1) {
            struct.set(TAG_MATCH_FUNC, tagMatchFunction);
        }
        List<Struct> topicArray = new ArrayList<>();
        for (TopicAndPartitionData<List<TagEntry>> topicEntry : topicsData) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, List<TagEntry>> partitionEntry : topicEntry.partitions.entrySet()) {
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                List<TagEntry> fetchPartitionTags = partitionEntry.getValue();
                List<Struct> tagStructs = new ArrayList<>();
                if (null != fetchPartitionTags) {
                    fetchPartitionTags.forEach(tagEntry -> {
                        Struct tagStruct = partitionData.instance(FETCH_TAGS_KEY_NAME);
                        buildTagListStruct(tagEntry, tagStruct);
                        tagStructs.add(tagStruct);
                    });
                }
                partitionData.set(FETCH_TAGS_KEY_NAME, tagStructs.toArray());
                TopicPartition topicPartition = new TopicPartition(topicEntry.topic, partitionEntry.getKey());
                buildStructFromSpecialTypeFilterTag(partitionData, specialTypeFilterTags.getOrDefault(topicPartition, SpecialTypeFilterTag.DEFAULT_SPECIAL_TYPE_TAG));
                buildStructFromHashFilterTag(partitionData, hashTypeFilterTags.getOrDefault(topicPartition, HashTypeFilterTag.DEFAULT_HASH_TYPE_TAG));
                buildStructFromUnitTypeFilterTag(partitionData, unitTypeFilterTags.getOrDefault(topicPartition, UnitTypeFilterTag.DEFAULT_UNIT_FILTER_TAG));
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        LinkedHashMap<TopicPartition, SetFetchTagsResponse.PartitionResponse> responseData = new LinkedHashMap<>();

        for (Map.Entry<TopicPartition, List<TagEntry>> entry: fetchTags.entrySet()) {
            SetFetchTagsResponse.PartitionResponse partitionResponse = new SetFetchTagsResponse.PartitionResponse(Errors.forException(e).code());

            responseData.put(entry.getKey(), partitionResponse);
        }
        short versionId = version();
        return new SetFetchTagsResponse(responseData, versionId);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(e);
    }

    public int replicaId() {
        return replicaId;
    }

    public Map<TopicPartition, List<TagEntry>> fetchTags() {
        return fetchTags;
    }
    public Map<TopicPartition, SpecialTypeFilterTag> getSpecialTypeFilterTags() {
        return specialTypeFilterTags;
    }
    public Map<TopicPartition, HashTypeFilterTag> getHashTypeFilterTags() {
        return hashTypeFilterTags;
    }

    public Map<TopicPartition, UnitTypeFilterTag> getUnitTypeFilterTags() {
        return unitTypeFilterTags;
    }

    public SpecialTypeFilterTag getSpecialTypeFilterTag(TopicPartition tp) {
        return specialTypeFilterTags.getOrDefault(tp, SpecialTypeFilterTag.DEFAULT_SPECIAL_TYPE_TAG);
    }
    public HashTypeFilterTag getHashTypeFilterTag(TopicPartition tp) {
        return hashTypeFilterTags.getOrDefault(tp, HashTypeFilterTag.DEFAULT_HASH_TYPE_TAG);
    }

    public UnitTypeFilterTag getUnitTypeFilterTag(TopicPartition tp) {
        return unitTypeFilterTags.getOrDefault(tp, UnitTypeFilterTag.DEFAULT_UNIT_FILTER_TAG);
    }

    public int getTagMatchFunction() {
        return tagMatchFunction;
    }

    public boolean isFromFollower() {
        return replicaId >= 0;
    }

    public static SetFetchTagsRequest parse(ByteBuffer buffer, int versionId) {
        return new SetFetchTagsRequest(ApiKeys.SET_FETCH_TAGS.parseRequest((short)versionId, buffer), (short) versionId);
    }

    public static SetFetchTagsRequest parse(ByteBuffer buffer) {
        return parse(buffer, ApiKeys.SET_FETCH_TAGS.latestVersion());
    }


}
