package org.apache.kafka.common.requests;


import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.*;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class ApiVersionsRequestData implements ApiMessage {
    private String clientSoftwareName;
    private String clientSoftwareVersion;
    private List<RawTaggedField> _unknownTaggedFields;

    public static final Schema SCHEMA_0 =
            new Schema(
            );

    public static final Schema SCHEMA_1 = SCHEMA_0;

    public static final Schema SCHEMA_2 = SCHEMA_1;

    public static final Schema SCHEMA_3 =
            new Schema(
                    new Field("client_software_name", Type.COMPACT_STRING, "The name of the client."),
                    new Field("client_software_version", Type.COMPACT_STRING, "The version of the client."),
                    Field.TaggedFieldsSection.of(
                    )
            );

    public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2,
            SCHEMA_3
    };

    public ApiVersionsRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }

    public ApiVersionsRequestData(Struct struct, short _version) {
        fromStruct(struct, _version);
    }

    public ApiVersionsRequestData() {
        this.clientSoftwareName = "";
        this.clientSoftwareVersion = "";
    }

    @Override
    public short apiKey() {
        return 18;
    }

    @Override
    public short lowestSupportedVersion() {
        return 0;
    }

    @Override
    public short highestSupportedVersion() {
        return 3;
    }

    @Override
    public void read(Readable _readable, short _version) {
        if (_version >= 3) {
            int length;
            length = _readable.readUnsignedVarint() - 1;
            if (length < 0) {
                throw new RuntimeException("non-nullable field clientSoftwareName was serialized as null");
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field clientSoftwareName had invalid length " + length);
            } else {
                this.clientSoftwareName = _readable.readString(length);
            }
        } else {
            this.clientSoftwareName = "";
        }
        if (_version >= 3) {
            int length;
            length = _readable.readUnsignedVarint() - 1;
            if (length < 0) {
                throw new RuntimeException("non-nullable field clientSoftwareVersion was serialized as null");
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field clientSoftwareVersion had invalid length " + length);
            } else {
                this.clientSoftwareVersion = _readable.readString(length);
            }
        } else {
            this.clientSoftwareVersion = "";
        }
        this._unknownTaggedFields = null;
        if (_version >= 3) {
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
        if (_version >= 3) {
            {
                byte[] _stringBytes = _cache.getSerializedValue(clientSoftwareName);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
        } else {
            if (!clientSoftwareName.equals("")) {
                throw new UnsupportedVersionException("Attempted to write a non-default clientSoftwareName at version " + _version);
            }
        }
        if (_version >= 3) {
            {
                byte[] _stringBytes = _cache.getSerializedValue(clientSoftwareVersion);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
        } else {
            if (!clientSoftwareVersion.equals("")) {
                throw new UnsupportedVersionException("Attempted to write a non-default clientSoftwareVersion at version " + _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 3) {
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
        if (_version >= 3) {
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
        }
        if (_version >= 3) {
            this.clientSoftwareName = struct.getString("client_software_name");
        } else {
            this.clientSoftwareName = "";
        }
        if (_version >= 3) {
            this.clientSoftwareVersion = struct.getString("client_software_version");
        } else {
            this.clientSoftwareVersion = "";
        }
        if (_version >= 3) {
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
        TreeMap<Integer, Object> _taggedFields = null;
        if (_version >= 3) {
            _taggedFields = new TreeMap<>();
        }
        Struct struct = new Struct(SCHEMAS[_version]);
        if (_version >= 3) {
            struct.set("client_software_name", this.clientSoftwareName);
        }
        if (_version >= 3) {
            struct.set("client_software_version", this.clientSoftwareVersion);
        }
        if (_version >= 3) {
            struct.set("_tagged_fields", _taggedFields);
        }
        return struct;
    }

    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        if (_version >= 3) {
            {
                byte[] _stringBytes = clientSoftwareName.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'clientSoftwareName' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(clientSoftwareName, _stringBytes);
                _size += _stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
            }
        }
        if (_version >= 3) {
            {
                byte[] _stringBytes = clientSoftwareVersion.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'clientSoftwareVersion' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(clientSoftwareVersion, _stringBytes);
                _size += _stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
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
        if (_version >= 3) {
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
        if (!(obj instanceof ApiVersionsRequestData)) return false;
        ApiVersionsRequestData other = (ApiVersionsRequestData) obj;
        if (this.clientSoftwareName == null) {
            if (other.clientSoftwareName != null) return false;
        } else {
            if (!this.clientSoftwareName.equals(other.clientSoftwareName)) return false;
        }
        if (this.clientSoftwareVersion == null) {
            if (other.clientSoftwareVersion != null) return false;
        } else {
            if (!this.clientSoftwareVersion.equals(other.clientSoftwareVersion)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (clientSoftwareName == null ? 0 : clientSoftwareName.hashCode());
        hashCode = 31 * hashCode + (clientSoftwareVersion == null ? 0 : clientSoftwareVersion.hashCode());
        return hashCode;
    }

    @Override
    public String toString() {
        return "ApiVersionsRequestData("
                + "clientSoftwareName=" + ((clientSoftwareName == null) ? "null" : "'" + clientSoftwareName.toString() + "'")
                + ", clientSoftwareVersion=" + ((clientSoftwareVersion == null) ? "null" : "'" + clientSoftwareVersion.toString() + "'")
                + ")";
    }

    public String clientSoftwareName() {
        return this.clientSoftwareName;
    }

    public String clientSoftwareVersion() {
        return this.clientSoftwareVersion;
    }

    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }

    public ApiVersionsRequestData setClientSoftwareName(String v) {
        this.clientSoftwareName = v;
        return this;
    }

    public ApiVersionsRequestData setClientSoftwareVersion(String v) {
        this.clientSoftwareVersion = v;
        return this;
    }
}
