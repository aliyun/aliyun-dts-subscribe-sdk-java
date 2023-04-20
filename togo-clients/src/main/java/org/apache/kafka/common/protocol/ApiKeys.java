/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.*;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.protocol.types.Type.*;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    PRODUCE(0, "Produce", ProduceRequest.schemaVersions(), ProduceResponse.schemaVersions()),
    FETCH(1, "Fetch", FetchRequest.schemaVersions(), FetchResponse.schemaVersions()),
    LIST_OFFSETS(2, "ListOffsets", ListOffsetRequest.schemaVersions(), ListOffsetResponse.schemaVersions()),
    METADATA(3, "Metadata", MetadataRequestData.SCHEMAS, MetadataResponseData.schemaVersions()),
    LEADER_AND_ISR(4, "LeaderAndIsr", true, LeaderAndIsrRequest.schemaVersions(), LeaderAndIsrResponse.schemaVersions()),
    STOP_REPLICA(5, "StopReplica", true, StopReplicaRequest.schemaVersions(), StopReplicaResponse.schemaVersions()),
    UPDATE_METADATA(6, "UpdateMetadata", true, UpdateMetadataRequest.schemaVersions(),
            UpdateMetadataResponse.schemaVersions()),
    CONTROLLED_SHUTDOWN(7, "ControlledShutdown", true, ControlledShutdownRequest.schemaVersions(),
            ControlledShutdownResponse.schemaVersions()),
    OFFSET_COMMIT(8, "OffsetCommit", OffsetCommitRequest.schemaVersions(), OffsetCommitResponse.schemaVersions()),
    OFFSET_FETCH(9, "OffsetFetch", OffsetFetchRequest.schemaVersions(), OffsetFetchResponse.schemaVersions()),
    FIND_COORDINATOR(10, "FindCoordinator", FindCoordinatorRequest.schemaVersions(),
            FindCoordinatorResponse.schemaVersions()),
    JOIN_GROUP(11, "JoinGroup", JoinGroupRequest.schemaVersions(), JoinGroupResponse.schemaVersions()),
    HEARTBEAT(12, "Heartbeat", HeartbeatRequest.schemaVersions(), HeartbeatResponse.schemaVersions()),
    LEAVE_GROUP(13, "LeaveGroup", LeaveGroupRequest.schemaVersions(), LeaveGroupResponse.schemaVersions()),
    SYNC_GROUP(14, "SyncGroup", SyncGroupRequest.schemaVersions(), SyncGroupResponse.schemaVersions()),
    DESCRIBE_GROUPS(15, "DescribeGroups", DescribeGroupsRequest.schemaVersions(),
            DescribeGroupsResponse.schemaVersions()),
    LIST_GROUPS(16, "ListGroups", ListGroupsRequest.schemaVersions(), ListGroupsResponse.schemaVersions()),
    SASL_HANDSHAKE(17, "SaslHandshake", SaslHandshakeRequest.schemaVersions(), SaslHandshakeResponse.schemaVersions()),
    API_VERSIONS(18, "ApiVersions", ApiVersionsRequestData.SCHEMAS, ApiVersionsResponseData.SCHEMAS) {
        @Override
        public Struct parseResponse(short version, ByteBuffer buffer) {
            // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
            // using a version higher than that supported by the broker, a version 0 response is sent
            // to the client indicating UNSUPPORTED_VERSION.
            return parseResponse(version, buffer, (short) 0);
        }
    },
    CREATE_TOPICS(19, "CreateTopics", CreateTopicsRequest.schemaVersions(), CreateTopicsResponse.schemaVersions()),
    DELETE_TOPICS(20, "DeleteTopics", DeleteTopicsRequest.schemaVersions(), DeleteTopicsResponse.schemaVersions()),
    DELETE_RECORDS(21, "DeleteRecords", DeleteRecordsRequest.schemaVersions(), DeleteRecordsResponse.schemaVersions()),
    INIT_PRODUCER_ID(22, "InitProducerId", InitProducerIdRequest.schemaVersions(),
            InitProducerIdResponse.schemaVersions()),
    OFFSET_FOR_LEADER_EPOCH(23, "OffsetForLeaderEpoch", true, OffsetsForLeaderEpochRequest.schemaVersions(),
            OffsetsForLeaderEpochResponse.schemaVersions()),
    ADD_PARTITIONS_TO_TXN(24, "AddPartitionsToTxn", false, RecordBatch.MAGIC_VALUE_V2,
            AddPartitionsToTxnRequest.schemaVersions(), AddPartitionsToTxnResponse.schemaVersions()),
    ADD_OFFSETS_TO_TXN(25, "AddOffsetsToTxn", false, RecordBatch.MAGIC_VALUE_V2, AddOffsetsToTxnRequest.schemaVersions(),
            AddOffsetsToTxnResponse.schemaVersions()),
    END_TXN(26, "EndTxn", false, RecordBatch.MAGIC_VALUE_V2, EndTxnRequest.schemaVersions(),
            EndTxnResponse.schemaVersions()),
    WRITE_TXN_MARKERS(27, "WriteTxnMarkers", true, RecordBatch.MAGIC_VALUE_V2, WriteTxnMarkersRequest.schemaVersions(),
            WriteTxnMarkersResponse.schemaVersions()),
    TXN_OFFSET_COMMIT(28, "TxnOffsetCommit", false, RecordBatch.MAGIC_VALUE_V2, TxnOffsetCommitRequest.schemaVersions(),
            TxnOffsetCommitResponse.schemaVersions()),
    DESCRIBE_ACLS(29, "DescribeAcls", DescribeAclsRequest.schemaVersions(), DescribeAclsResponse.schemaVersions()),
    CREATE_ACLS(30, "CreateAcls", CreateAclsRequest.schemaVersions(), CreateAclsResponse.schemaVersions()),
    DELETE_ACLS(31, "DeleteAcls", DeleteAclsRequest.schemaVersions(), DeleteAclsResponse.schemaVersions()),
    DESCRIBE_CONFIGS(32, "DescribeConfigs", DescribeConfigsRequest.schemaVersions(),
            DescribeConfigsResponse.schemaVersions()),
    ALTER_CONFIGS(33, "AlterConfigs", AlterConfigsRequest.schemaVersions(),
            AlterConfigsResponse.schemaVersions()),
    ALTER_REPLICA_LOG_DIRS(34, "AlterReplicaLogDirs", AlterReplicaLogDirsRequest.schemaVersions(),
            AlterReplicaLogDirsResponse.schemaVersions()),
    DESCRIBE_LOG_DIRS(35, "DescribeLogDirs", DescribeLogDirsRequest.schemaVersions(),
            DescribeLogDirsResponse.schemaVersions()),
    SASL_AUTHENTICATE(36, "SaslAuthenticate", SaslAuthenticateRequest.schemaVersions(),
            SaslAuthenticateResponse.schemaVersions()),
    CREATE_PARTITIONS(37, "CreatePartitions", CreatePartitionsRequest.schemaVersions(),
            CreatePartitionsResponse.schemaVersions()),
    // TOGO extend apis
    CREATE_RECORD_METAS(255, "CreateRecordMetas", CreateRecordMetasRequest.schemaVersions(), CreateRecordMetasResponse.schemaVersions()),
    FETCH_RECORD_METAS(256, "FetchRecordMetas", FetchRecordMetasRequest.schemaVersions(), FetchRecordMetasResponse.schemaVersions()),
    FLEX_LIST_OFFSET(257, "FlexListOffset", FlexListOffsetRequest.schemaVersions(), FlexListOffsetResponse.schemaVersions()),
    SET_FETCH_TAGS(258, "SetFetchTags", SetFetchTagsRequest.schemaVersions(), SetFetchTagsResponse.schemaVersions()),
    RELOAD_AUTH_META(259, "ReloadAuthMeta", ReloadAuthMetaRequest.schemaVersions(), ReloadAuthMetaResponse.schemaVersions());

    private static final ApiKeys[] ID_TO_TYPE;
    private static final int MIN_API_KEY = 0;
    public static final int MAX_API_KEY;

    static {
        int maxKey = -1;
        for (ApiKeys key : ApiKeys.values())
            maxKey = Math.max(maxKey, key.id);
        ApiKeys[] idToType = new ApiKeys[maxKey + 1];
        for (ApiKeys key : ApiKeys.values())
            idToType[key.id] = key;
        ID_TO_TYPE = idToType;
        MAX_API_KEY = maxKey;
    }

    /** the permanent and immutable id of an API--this can't change ever */
    public final short id;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    /** indicates if this is a ClusterAction request used only by brokers */
    public final boolean clusterAction;

    /** indicates the minimum required inter broker magic required to support the API */
    public final byte minRequiredInterBrokerMagic;

    public final Schema[] requestSchemas;
    public final Schema[] responseSchemas;
    public final boolean requiresDelayedAllocation;

    ApiKeys(int id, String name, Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, false, requestSchemas, responseSchemas);
    }

    ApiKeys(int id, String name, boolean clusterAction, Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, clusterAction, RecordBatch.MAGIC_VALUE_V0, requestSchemas, responseSchemas);
    }

    ApiKeys(int id, String name, boolean clusterAction, byte minRequiredInterBrokerMagic,
            Schema[] requestSchemas, Schema[] responseSchemas) {
        if (id < 0)
            throw new IllegalArgumentException("id must not be negative, id: " + id);
        this.id = (short) id;
        this.name = name;
        this.clusterAction = clusterAction;
        this.minRequiredInterBrokerMagic = minRequiredInterBrokerMagic;

        if (requestSchemas.length != responseSchemas.length)
            throw new IllegalStateException(requestSchemas.length + " request versions for api " + name
                    + " but " + responseSchemas.length + " response versions.");

        for (int i = 0; i < requestSchemas.length; ++i) {
            if (requestSchemas[i] == null)
                throw new IllegalStateException("Request schema for api " + name + " for version " + i + " is null");
            if (responseSchemas[i] == null)
                throw new IllegalStateException("Response schema for api " + name + " for version " + i + " is null");
        }

        boolean requestRetainsBufferReference = false;
        for (Schema requestVersionSchema : requestSchemas) {
            if (retainsBufferReference(requestVersionSchema)) {
                requestRetainsBufferReference = true;
                break;
            }
        }
        this.requiresDelayedAllocation = requestRetainsBufferReference;
        this.requestSchemas = requestSchemas;
        this.responseSchemas = responseSchemas;
    }

    public static ApiKeys forId(int id) {
        if (!hasId(id))
            throw new IllegalArgumentException(String.format("Unexpected ApiKeys id `%s`, it should be between `%s` " +
                    "and `%s` (inclusive)", id, MIN_API_KEY, MAX_API_KEY));
        return ID_TO_TYPE[id];
    }

    public static boolean hasId(int id) {
        return (id >= MIN_API_KEY && id <= CREATE_PARTITIONS.id) || (id >= CREATE_RECORD_METAS.id && id <= MAX_API_KEY);
    }

    public short latestVersion() {
        return (short) (requestSchemas.length - 1);
    }

    public short oldestVersion() {
        return 0;
    }

    public Schema requestSchema(short version) {
        return schemaFor(requestSchemas, version);
    }

    public Schema responseSchema(short version) {
        return schemaFor(responseSchemas, version);
    }

    public Struct parseRequest(short version, ByteBuffer buffer) {
        return requestSchema(version).read(buffer);
    }

    public Struct parseResponse(short version, ByteBuffer buffer) {
        return responseSchema(version).read(buffer);
    }

    protected Struct parseResponse(short version, ByteBuffer buffer, short fallbackVersion) {
        int bufferPosition = buffer.position();
        try {
            return responseSchema(version).read(buffer);
        } catch (SchemaException e) {
            if (version != fallbackVersion) {
                buffer.position(bufferPosition);
                return responseSchema(fallbackVersion).read(buffer);
            } else
                throw e;
        }
    }

    private Schema schemaFor(Schema[] versions, short version) {
        if (!isVersionSupported(version))
            throw new IllegalArgumentException("Invalid version for API key " + this + ": " + version);
        return versions[version];
    }

    public boolean isVersionSupported(short apiVersion) {
        return apiVersion >= oldestVersion() && apiVersion <= latestVersion();
    }

    public short requestHeaderVersion(short apiVersion) {
        return ApiKeys.forId(id).realRequestHeaderVersion(apiVersion);
    }

    public short responseHeaderVersion(short apiVersion) {
        return ApiKeys.forId(id).realResponseHeaderVersion(apiVersion);
    }

    public short realRequestHeaderVersion(short _version) {
        switch (id) {
            case 0:
                return (short) 1;
            case 1:
                return (short) 1;
            case 2:
                return (short) 1;
            case 3:
                if (_version >= 9 && _version <= 62) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 4:
                if (_version >= 4) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 5:
                if (_version >= 2) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 6:
                if (_version >= 6) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 7:
                if (_version == 0) {
                    return (short) 0;
                }
                if (_version >= 3) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 8:
                if (_version >= 8 && _version <= 62) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 9:
                if (_version >= 6) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 10:
                if (_version >= 3) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 11:
                if (_version >= 6) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 12:
                if (_version >= 4) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 13:
                if (_version >= 4) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 14:
                if (_version >= 4) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 15:
                if (_version >= 5) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 16:
                if (_version >= 3) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 17:
                return (short) 1;
            case 18:
                if (_version >= 3) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 19:
                if (_version >= 5) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 20:
                if (_version >= 4) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 21:
                return (short) 1;
            case 22:
                if (_version >= 2) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 23:
                return (short) 1;
            case 24:
                return (short) 1;
            case 25:
                return (short) 1;
            case 26:
                return (short) 1;
            case 27:
                return (short) 1;
            case 28:
                return (short) 1;
            case 29:
                return (short) 1;
            case 30:
                return (short) 1;
            case 31:
                return (short) 1;
            case 32:
                return (short) 1;
            case 33:
                return (short) 1;
            case 34:
                return (short) 1;
            case 35:
                return (short) 1;
            case 36:
                return (short) 1;
            case 37:
                return (short) 1;
            case 38:
                if (_version >= 2) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 39:
                return (short) 1;
            case 40:
                return (short) 1;
            case 41:
                return (short) 1;
            case 42:
                if (_version >= 2) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 43:
                if (_version >= 2) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 44:
                if (_version >= 1) {
                    return (short) 2;
                } else {
                    return (short) 1;
                }
            case 45:
                return (short) 2;
            case 46:
                return (short) 2;
            case 47:
                return (short) 1;
            default:
                return (short) 1;
        }
    }

    public short realResponseHeaderVersion(short _version) {
        switch (id) {
            case 0:
                return (short) 0;
            case 1:
                return (short) 0;
            case 2:
                return (short) 0;
            case 3:
                if (_version >= 9 && _version <= 62) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 4:
                if (_version >= 4) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 5:
                if (_version >= 2) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 6:
                if (_version >= 6) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 7:
                if (_version >= 3) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 8:
                if (_version >= 8 && _version <= 62) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 9:
                if (_version >= 6) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 10:
                if (_version >= 3) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 11:
                if (_version >= 6) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 12:
                if (_version >= 4) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 13:
                if (_version >= 4) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 14:
                if (_version >= 4) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 15:
                if (_version >= 5) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 16:
                if (_version >= 3) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 17:
                return (short) 0;
            case 18:
                return (short) 0;
            case 19:
                if (_version >= 5) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 20:
                if (_version >= 4) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 21:
                return (short) 0;
            case 22:
                if (_version >= 2) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 23:
                return (short) 0;
            case 24:
                return (short) 0;
            case 25:
                return (short) 0;
            case 26:
                return (short) 0;
            case 27:
                return (short) 0;
            case 28:
                return (short) 0;
            case 29:
                return (short) 0;
            case 30:
                return (short) 0;
            case 31:
                return (short) 0;
            case 32:
                return (short) 0;
            case 33:
                return (short) 0;
            case 34:
                return (short) 0;
            case 35:
                return (short) 0;
            case 36:
                return (short) 0;
            case 37:
                return (short) 0;
            case 38:
                if (_version >= 2) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 39:
                return (short) 0;
            case 40:
                return (short) 0;
            case 41:
                return (short) 0;
            case 42:
                if (_version >= 2) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 43:
                if (_version >= 2) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 44:
                if (_version >= 1) {
                    return (short) 1;
                } else {
                    return (short) 0;
                }
            case 45:
                return (short) 1;
            case 46:
                return (short) 1;
            case 47:
                return (short) 0;
            default:
                return (short) 0;
        }
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Name</th>\n");
        b.append("<th>Key</th>\n");
        b.append("</tr>");
        for (ApiKeys key : ApiKeys.values()) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append("<a href=\"#The_Messages_" + key.name + "\">" + key.name + "</a>");
            b.append("</td>");
            b.append("<td>");
            b.append(key.id);
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</table>\n");
        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }

    private static boolean retainsBufferReference(Schema schema) {
        final AtomicBoolean hasBuffer = new AtomicBoolean(false);
        Schema.Visitor detector = new Schema.Visitor() {
            @Override
            public void visit(Type field) {
                if (field == BYTES || field == NULLABLE_BYTES || field == RECORDS)
                    hasBuffer.set(true);
            }
        };
        schema.walk(detector);
        return hasBuffer.get();
    }

}
