package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.protocol.types.*;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class MetadataRequestData  implements ApiMessage {
    private static final String DEFAULT_CONFIGS_KEY_NAME = "default_configs";

    private List<MetadataRequestTopic> topics;
    private boolean allowAutoTopicCreation;
    private boolean includeClusterAuthorizedOperations;
    private boolean includeTopicAuthorizedOperations;
    private List<RawTaggedField> _unknownTaggedFields;

    private List<String> defaultConfigs = null;

    public static final Schema SCHEMA_0 =
            new Schema(
                    new Field("topics", new ArrayOf(MetadataRequestTopic.SCHEMA_0), "The topics to fetch metadata for.")
            );

    public static final Schema SCHEMA_1 =
            new Schema(
                    new Field("topics", ArrayOf.nullable(MetadataRequestTopic.SCHEMA_0), "The topics to fetch metadata for.")
            );

    public static final Schema SCHEMA_2 = SCHEMA_1;

    public static final Schema SCHEMA_3 = SCHEMA_2;

    public static final Schema SCHEMA_4 =
            new Schema(
                    new Field("topics", ArrayOf.nullable(MetadataRequestTopic.SCHEMA_0), "The topics to fetch metadata for."),
                    new Field("allow_auto_topic_creation", Type.BOOLEAN, "If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.")
            );

    public static final Schema SCHEMA_5 = SCHEMA_4;

    public static final Schema SCHEMA_6 = SCHEMA_5;

    public static final Schema SCHEMA_7 = SCHEMA_6;

    public static final Schema SCHEMA_8 =
            new Schema(
                    new Field("topics", ArrayOf.nullable(MetadataRequestTopic.SCHEMA_0), "The topics to fetch metadata for."),
                    new Field("allow_auto_topic_creation", Type.BOOLEAN, "If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so."),
                    new Field("include_cluster_authorized_operations", Type.BOOLEAN, "Whether to include cluster authorized operations."),
                    new Field("include_topic_authorized_operations", Type.BOOLEAN, "Whether to include topic authorized operations.")
            );

    public static final Schema SCHEMA_9 =
            new Schema(
                    new Field("topics", CompactArrayOf.nullable(MetadataRequestTopic.SCHEMA_9), "The topics to fetch metadata for."),
                    new Field("allow_auto_topic_creation", Type.BOOLEAN, "If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so."),
                    new Field("include_cluster_authorized_operations", Type.BOOLEAN, "Whether to include cluster authorized operations."),
                    new Field("include_topic_authorized_operations", Type.BOOLEAN, "Whether to include topic authorized operations."),
                    Field.TaggedFieldsSection.of(
                    )
            );

    private static final Schema METADATA_REQUEST_V64 = new Schema(
            new Field("topics", ArrayOf.nullable(MetadataRequestTopic.METADATA_REQUEST_V64), "An array of topics to fetch meta for."),
            new Field("allow_auto_topic_creation", BOOLEAN, "If this and the broker config " +
                    "'auto.create.topics.enable' are true, topics that don't exist will be created by the broker. " +
                    "Otherwise, no topics will be created by the broker."),
            new Field("default_configs", ArrayOf.nullable(STRING),
                    "An array of default configs to create topic."));

    public static Schema[] schemaVersions() {
        return fillSchemaWithGap(new Schema[]{SCHEMA_0, SCHEMA_1, SCHEMA_2, SCHEMA_3,
                SCHEMA_4, SCHEMA_5, SCHEMA_6, SCHEMA_7, SCHEMA_8 , SCHEMA_9}, SCHEMA_9, 63, METADATA_REQUEST_V64);
    }

    public static final Schema[] SCHEMAS = schemaVersions();

    public static Schema[] fillSchemaWithGap(Schema[] originSchema, Schema gapSchema, int gapToSize, Schema endSchema) {
        List<Schema> retSchema = new ArrayList<>();
        retSchema.addAll(Arrays.asList(originSchema));
        while (retSchema.size() < gapToSize) {
            retSchema.add(gapSchema);
        }
        retSchema.add(endSchema);
        return retSchema.toArray(new Schema[retSchema.size()]);
    }

    public MetadataRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }

    public MetadataRequestData(Struct struct, short _version) {
        fromStruct(struct, _version);
    }

    public MetadataRequestData(List<MetadataRequestTopic> topics, boolean allowAutoTopicCreation, List<String> defaultConfigs) {
        this.topics = topics;
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        this.defaultConfigs = defaultConfigs;
    }

    public MetadataRequestData() {
        this.topics = new ArrayList<MetadataRequestTopic>();
        this.allowAutoTopicCreation = true;
        this.includeClusterAuthorizedOperations = false;
        this.includeTopicAuthorizedOperations = false;
    }

    @Override
    public short apiKey() {
        return 3;
    }

    @Override
    public short lowestSupportedVersion() {
        return 0;
    }

    @Override
    public short highestSupportedVersion() {
        return 64;
    }

    @Override
    public void read(Readable _readable, short _version) {
        {
            if (_version >= 9 && _version <= 62) {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    this.topics = null;
                } else {
                    ArrayList<MetadataRequestTopic> newCollection = new ArrayList<MetadataRequestTopic>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new MetadataRequestTopic(_readable, _version));
                    }
                    this.topics = newCollection;
                }
            } else {
                int arrayLength;
                arrayLength = _readable.readInt();
                if (arrayLength < 0) {
                    if (_version >= 1) {
                        this.topics = null;
                    } else {
                        throw new RuntimeException("non-nullable field topics was serialized as null");
                    }
                } else {
                    ArrayList<MetadataRequestTopic> newCollection = new ArrayList<MetadataRequestTopic>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new MetadataRequestTopic(_readable, _version));
                    }
                    this.topics = newCollection;
                }
            }
        }
        if (_version >= 4) {
            this.allowAutoTopicCreation = _readable.readByte() != 0;
        } else {
            this.allowAutoTopicCreation = true;
        }
        if (_version >= 8 && _version <= 62) {
            this.includeClusterAuthorizedOperations = _readable.readByte() != 0;
        } else {
            this.includeClusterAuthorizedOperations = false;
        }
        if (_version >= 8 && _version <= 62) {
            this.includeTopicAuthorizedOperations = _readable.readByte() != 0;
        } else {
            this.includeTopicAuthorizedOperations = false;
        }
        this._unknownTaggedFields = null;
        if (_version >= 9 && _version <= 62) {
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
    }

    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        if (_version >= 9 && _version <= 62) {
            if (topics == null) {
                _writable.writeUnsignedVarint(0);
            } else {
                _writable.writeUnsignedVarint(topics.size() + 1);
                for (MetadataRequestTopic topicsElement : topics) {
                    topicsElement.write(_writable, _cache, _version);
                }
            }
        } else {
            if (topics == null) {
                if (_version >= 1) {
                    _writable.writeInt(-1);
                } else {
                    throw new NullPointerException();
                }
            } else {
                _writable.writeInt(topics.size());
                for (MetadataRequestTopic topicsElement : topics) {
                    topicsElement.write(_writable, _cache, _version);
                }
            }
        }
        if (_version >= 4) {
            _writable.writeByte(allowAutoTopicCreation ? (byte) 1 : (byte) 0);
        } else {
            if (!allowAutoTopicCreation) {
                throw new UnsupportedVersionException("Attempted to write a non-default allowAutoTopicCreation at version " + _version);
            }
        }
        if (_version >= 8 && _version <= 62) {
            _writable.writeByte(includeClusterAuthorizedOperations ? (byte) 1 : (byte) 0);
        } else {
            if (includeClusterAuthorizedOperations) {
                throw new UnsupportedVersionException("Attempted to write a non-default includeClusterAuthorizedOperations at version " + _version);
            }
        }
        if (_version >= 8 && _version <= 62) {
            _writable.writeByte(includeTopicAuthorizedOperations ? (byte) 1 : (byte) 0);
        } else {
            if (includeTopicAuthorizedOperations) {
                throw new UnsupportedVersionException("Attempted to write a non-default includeTopicAuthorizedOperations at version " + _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 9 && _version <= 62) {
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void fromStruct(Struct struct, short _version) {
        NavigableMap<Integer, Object> _taggedFields = null;
        this._unknownTaggedFields = null;
        if (_version >= 9 && _version <= 62) {
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
        }
        {
            Object[] _nestedObjects = struct.getArray("topics");
            if (_nestedObjects == null) {
                this.topics = null;
            } else {
                this.topics = new ArrayList<MetadataRequestTopic>(_nestedObjects.length);
                for (Object nestedObject : _nestedObjects) {
                    this.topics.add(new MetadataRequestTopic((Struct) nestedObject, _version));
                }
            }
        }
        if (_version >= 4) {
            this.allowAutoTopicCreation = struct.getBoolean("allow_auto_topic_creation");
        } else {
            this.allowAutoTopicCreation = true;
        }
        if (_version >= 8 && _version <= 62) {
            this.includeClusterAuthorizedOperations = struct.getBoolean("include_cluster_authorized_operations");
        } else {
            this.includeClusterAuthorizedOperations = false;
        }
        if (_version >= 8 && _version <= 62) {
            this.includeTopicAuthorizedOperations = struct.getBoolean("include_topic_authorized_operations");
        } else {
            this.includeTopicAuthorizedOperations = false;
        }
        if (_version >= 9 && _version <= 62) {
            if (!_taggedFields.isEmpty()) {
                this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                for (Map.Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                    this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                }
            }
        }

        Function<String, List<String>> parseStringList = keyName -> {
            if (!struct.hasField(keyName)) {
                return null;
            }
            Object[] stringArray = struct.getArray(keyName);
            if (stringArray != null) {
                List<String> stringList = new ArrayList<>();
                for (Object stringObject : stringArray) {
                    stringList.add((String) stringObject);
                }
                return stringList;
            } else {
                return null;
            }
        };

        defaultConfigs = parseStringList.apply(DEFAULT_CONFIGS_KEY_NAME);
    }

    @Override
    public Struct toStruct(short _version) {
        TreeMap<Integer, Object> _taggedFields = null;
        if (_version >= 9 && _version <= 62) {
            _taggedFields = new TreeMap<>();
        }
        Struct struct = new Struct(SCHEMAS[_version]);
        {
            if (topics == null) {
                struct.set("topics", null);
            } else {
                Struct[] _nestedObjects = new Struct[topics.size()];
                int i = 0;
                for (MetadataRequestTopic element : this.topics) {
                    _nestedObjects[i++] = element.toStruct(_version);
                }
                struct.set("topics", (Object[]) _nestedObjects);
            }
        }
        if (_version >= 4) {
            struct.set("allow_auto_topic_creation", this.allowAutoTopicCreation);
        }
        if (_version >= 8 && _version <= 62) {
            struct.set("include_cluster_authorized_operations", this.includeClusterAuthorizedOperations);
        }
        if (_version >= 8 && _version <= 62) {
            struct.set("include_topic_authorized_operations", this.includeTopicAuthorizedOperations);
        }
        if (_version >= 9 && _version <= 62) {
            struct.set("_tagged_fields", _taggedFields);
        }

        //dts default_configs
        if (struct.hasField(DEFAULT_CONFIGS_KEY_NAME) && null != defaultConfigs) {
            struct.set(DEFAULT_CONFIGS_KEY_NAME, defaultConfigs.toArray());
        }

        return struct;
    }

    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        if (topics == null) {
            if (_version >= 9 && _version <= 62) {
                _size += 1;
            } else {
                _size += 4;
            }
        } else {
            int _arraySize = 0;
            if (_version >= 9 && _version <= 62) {
                _arraySize += ByteUtils.sizeOfUnsignedVarint(topics.size() + 1);
            } else {
                _arraySize += 4;
            }
            for (MetadataRequestTopic topicsElement : topics) {
                _arraySize += topicsElement.size(_cache, _version);
            }
            _size += _arraySize;
        }
        if (_version >= 4) {
            _size += 1;
        }
        if (_version >= 8 && _version <= 62) {
            _size += 1;
        }
        if (_version >= 8 && _version <= 62) {
            _size += 1;
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                _size += _field.size();
            }
        }
        if (_version >= 9 && _version <= 62) {
            _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
        return _size;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MetadataRequestData)) return false;
        MetadataRequestData other = (MetadataRequestData) obj;
        if (this.topics == null) {
            if (other.topics != null) return false;
        } else {
            if (!this.topics.equals(other.topics)) return false;
        }
        if (allowAutoTopicCreation != other.allowAutoTopicCreation) return false;
        if (includeClusterAuthorizedOperations != other.includeClusterAuthorizedOperations) return false;
        if (includeTopicAuthorizedOperations != other.includeTopicAuthorizedOperations) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (topics == null ? 0 : topics.hashCode());
        hashCode = 31 * hashCode + (allowAutoTopicCreation ? 1231 : 1237);
        hashCode = 31 * hashCode + (includeClusterAuthorizedOperations ? 1231 : 1237);
        hashCode = 31 * hashCode + (includeTopicAuthorizedOperations ? 1231 : 1237);
        return hashCode;
    }

    @Override
    public String toString() {
        return "MetadataRequestData("
                + "topics=" + ((topics == null) ? "null" : MessageUtil.deepToString(topics.iterator()))
                + ", allowAutoTopicCreation=" + (allowAutoTopicCreation ? "true" : "false")
                + ", includeClusterAuthorizedOperations=" + (includeClusterAuthorizedOperations ? "true" : "false")
                + ", includeTopicAuthorizedOperations=" + (includeTopicAuthorizedOperations ? "true" : "false")
                + ")";
    }

    public List<MetadataRequestTopic> topics() {
        return this.topics;
    }

    public boolean allowAutoTopicCreation() {
        return this.allowAutoTopicCreation;
    }

    public boolean includeClusterAuthorizedOperations() {
        return this.includeClusterAuthorizedOperations;
    }

    public boolean includeTopicAuthorizedOperations() {
        return this.includeTopicAuthorizedOperations;
    }

    public List<String>  defaultConfigs() {
        return this.defaultConfigs;
    }

    public void defaultConfigs(List<String> configs) {
        this.defaultConfigs = configs;
    }

    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }

    public MetadataRequestData setTopics(List<MetadataRequestTopic> v) {
        this.topics = v;
        return this;
    }

    public MetadataRequestData setAllowAutoTopicCreation(boolean v) {
        this.allowAutoTopicCreation = v;
        return this;
    }

    public MetadataRequestData setIncludeClusterAuthorizedOperations(boolean v) {
        this.includeClusterAuthorizedOperations = v;
        return this;
    }

    public MetadataRequestData setIncludeTopicAuthorizedOperations(boolean v) {
        this.includeTopicAuthorizedOperations = v;
        return this;
    }

    static public class MetadataRequestTopic implements Message {
        private String name;
        private List<RawTaggedField> _unknownTaggedFields;

        public static final Schema SCHEMA_0 =
                new Schema(
                        new Field("name", Type.STRING, "The topic name.")
                );

        public static final Schema SCHEMA_1 = SCHEMA_0;

        public static final Schema SCHEMA_2 = SCHEMA_1;

        public static final Schema SCHEMA_3 = SCHEMA_2;

        public static final Schema SCHEMA_4 = SCHEMA_3;

        public static final Schema SCHEMA_5 = SCHEMA_4;

        public static final Schema SCHEMA_6 = SCHEMA_5;

        public static final Schema SCHEMA_7 = SCHEMA_6;

        public static final Schema SCHEMA_8 = SCHEMA_7;

        public static final Schema SCHEMA_9 =
                new Schema(
                        new Field("name", Type.COMPACT_STRING, "The topic name."),
                        Field.TaggedFieldsSection.of(
                        )
                );

        public static final Schema METADATA_REQUEST_V64 = SCHEMA_0;

        public static Schema[] schemaVersions() {
            return fillSchemaWithGap(new Schema[]{SCHEMA_0, SCHEMA_1, SCHEMA_2, SCHEMA_3,
                    SCHEMA_4, SCHEMA_5, SCHEMA_6, SCHEMA_7, SCHEMA_8 , SCHEMA_9}, SCHEMA_9, 63, METADATA_REQUEST_V64);
        }

        public static final Schema[] SCHEMAS = schemaVersions();

        public MetadataRequestTopic(Readable _readable, short _version) {
            read(_readable, _version);
        }

        public MetadataRequestTopic(Struct struct, short _version) {
            fromStruct(struct, _version);
        }

        public MetadataRequestTopic() {
            this.name = "";
        }


        @Override
        public short lowestSupportedVersion() {
            return 0;
        }

        @Override
        public short highestSupportedVersion() {
            return 9;
        }

        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 9 && _version <= 62) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of MetadataRequestTopic");
            }
            {
                int length;
                if (_version >= 9 && _version <= 62) {
                    length = _readable.readUnsignedVarint() - 1;
                } else {
                    length = _readable.readShort();
                }
                if (length < 0) {
                    throw new RuntimeException("non-nullable field name was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field name had invalid length " + length);
                } else {
                    this.name = _readable.readString(length);
                }
            }
            this._unknownTaggedFields = null;
            if (_version >= 9 && _version <= 62) {
                int _numTaggedFields = _readable.readUnsignedVarint();
                for (int _i = 0; _i < _numTaggedFields; _i++) {
                    int _tag = _readable.readUnsignedVarint();
                    int _size = _readable.readUnsignedVarint();
                    switch (_tag) {
                        default:
                            this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                            break;
                    }
                }
            }
        }

        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version > 9 && _version <= 62) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of MetadataRequestTopic");
            }
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(name);
                if (_version >= 9 && _version <= 62) {
                    _writable.writeUnsignedVarint(_stringBytes.length + 1);
                } else {
                    _writable.writeShort((short) _stringBytes.length);
                }
                _writable.writeByteArray(_stringBytes);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 9 && _version <= 62) {
                _writable.writeUnsignedVarint(_numTaggedFields);
                _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 9 && _version <= 62) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of MetadataRequestTopic");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            if (_version >= 9 && _version <= 62) {
                _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            }
            this.name = struct.getString("name");

            if (_version >= 9 && _version <= 62) {
                if (!_taggedFields.isEmpty()) {
                    this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                    for (Map.Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                        this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                    }
                }
            }
        }

        @Override
        public Struct toStruct(short _version) {
            if (_version > 9 && _version <= 62) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of MetadataRequestTopic");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            if (_version >= 9 && _version <= 62) {
                _taggedFields = new TreeMap<>();
            }
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("name", this.name);
            if (_version >= 9 && _version <= 62) {
                struct.set("_tagged_fields", _taggedFields);
            }
            return struct;
        }

        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 9 && _version <= 62) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of MetadataRequestTopic");
            }
            {
                byte[] _stringBytes = name.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'name' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(name, _stringBytes);
                if (_version >= 9 && _version <= 62) {
                    _size += _stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
                } else {
                    _size += _stringBytes.length + 2;
                }
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                    _size += _field.size();
                }
            }
            if (_version >= 9 && _version <= 62) {
                _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
            return _size;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MetadataRequestTopic)) return false;
            MetadataRequestTopic other = (MetadataRequestTopic) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
            return hashCode;
        }

        @Override
        public String toString() {
            return "MetadataRequestTopic("
                    + "name=" + ((name == null) ? "null" : "'" + name.toString() + "'")
                    + ")";
        }

        public String name() {
            return this.name;
        }

        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }

        public MetadataRequestTopic setName(String v) {
            this.name = v;
            return this;
        }
    }
}
