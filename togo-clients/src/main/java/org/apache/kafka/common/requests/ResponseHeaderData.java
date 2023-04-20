package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.*;
import org.apache.kafka.common.utils.ByteUtils;

import java.util.*;

public class ResponseHeaderData implements ApiMessage {
    private int correlationId;
    private List<RawTaggedField> _unknownTaggedFields;

    public static final Schema SCHEMA_0 =
            new Schema(
                    new Field("correlation_id", Type.INT32, "The correlation ID of this response.")
            );

    public static final Schema SCHEMA_1 =
            new Schema(
                    new Field("correlation_id", Type.INT32, "The correlation ID of this response."),
                    Field.TaggedFieldsSection.of(
                    )
            );

    public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1
    };

    public ResponseHeaderData(Readable _readable, short _version) {
        read(_readable, _version);
    }

    public ResponseHeaderData(Struct struct, short _version) {
        fromStruct(struct, _version);
    }

    public ResponseHeaderData() {
        this.correlationId = 0;
    }

    @Override
    public short apiKey() {
        return -1;
    }

    @Override
    public short lowestSupportedVersion() {
        return 0;
    }

    @Override
    public short highestSupportedVersion() {
        return 1;
    }

    @Override
    public void read(Readable _readable, short _version) {
        this.correlationId = _readable.readInt();
        this._unknownTaggedFields = null;
        if (_version >= 1) {
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
        _writable.writeInt(correlationId);
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 1) {
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
        if (_version >= 1) {
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
        }
        this.correlationId = struct.getInt("correlation_id");
        if (_version >= 1) {
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
        if (_version >= 1) {
            _taggedFields = new TreeMap<>();
        }
        Struct struct = new Struct(SCHEMAS[_version]);
        struct.set("correlation_id", this.correlationId);
        if (_version >= 1) {
            struct.set("_tagged_fields", _taggedFields);
        }
        return struct;
    }

    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        _size += 4;
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                _size += _field.size();
            }
        }
        if (_version >= 1) {
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
        if (!(obj instanceof ResponseHeaderData)) return false;
        ResponseHeaderData other = (ResponseHeaderData) obj;
        if (correlationId != other.correlationId) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + correlationId;
        return hashCode;
    }

    @Override
    public String toString() {
        return "ResponseHeaderData("
                + "correlationId=" + correlationId
                + ")";
    }

    public int correlationId() {
        return this.correlationId;
    }

    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }

    public ResponseHeaderData setCorrelationId(int v) {
        this.correlationId = v;
        return this;
    }
}
