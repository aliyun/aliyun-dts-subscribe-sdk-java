package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.*;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by longxuan on 18/2/23.
 */
public class ReloadAuthMetaRequest extends AbstractRequest {
    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String DISCONNECT_STRATEGY = "disconnect_strategy";
    private static final String DISCONNECT_LIST = "disconnect_list";

    public enum DisconnectStrategy {
        FORCE_DISCONNECT(0),
        NOT_DISCONNECT(1),
        DISCONNECT_IF_RELOAD_SUCCESS(2),
        DISCONNECT_BY_LIST(3);
        private int strategy;
        DisconnectStrategy(int strategy) {
            this.strategy = strategy;
        }
        public int getStrategy() {
            return this.strategy;
        }
    }

    public static DisconnectStrategy idToDisconnectStrategy(int id) {
        switch (id) {
            case 0:
                return DisconnectStrategy.FORCE_DISCONNECT;
            case 1:
                return DisconnectStrategy.NOT_DISCONNECT;
            case 2:
                return DisconnectStrategy.DISCONNECT_IF_RELOAD_SUCCESS;
            case 3:
                return DisconnectStrategy.DISCONNECT_BY_LIST;
            default:
                throw new RuntimeException("invalid id:" + id + " for disconnect strategy");
        }
    }

    private static final Schema RELOAD_REQUEST_SCHEMA_V0 = new Schema(new Field(REPLICA_ID_KEY_NAME, Type.INT32, "replica id"),
            new Field(DISCONNECT_STRATEGY, Type.INT32, " disconnect strategy when reload"),
            new Field(DISCONNECT_LIST, ArrayOf.nullable(Type.STRING), "disconnect list if given lists"));
    private static final Schema RELOAD_REQUEST_SCHEMA_V1 = RELOAD_REQUEST_SCHEMA_V0;

    private int replicaID;
    private int disconnectStrategy;
    private List<String> disconnectList;
    public int getReplicaID() {
        return replicaID;
    }
    public int getDisconnectStrategy() {
        return disconnectStrategy;
    }
    public List<String> getDisconnectList() {
        return disconnectList;
    }
    private ReloadAuthMetaRequest(int replicaID, int disconnectStrategy, List<String> disconnectList, short version) {
        super(version);
        this.replicaID = replicaID;
        this.disconnectStrategy = disconnectStrategy;
        this.disconnectList = disconnectList;
    }

    public ReloadAuthMetaRequest(Struct struct, short version) {
        super(version);
        replicaID = struct.getInt(REPLICA_ID_KEY_NAME);
        disconnectStrategy = struct.getInt(DISCONNECT_STRATEGY);
        disconnectList = new LinkedList<>();
        Object[] disconnectListArray = struct.getArray(DISCONNECT_LIST);
        if (null != disconnectListArray) {
            for (Object channelID : disconnectListArray) {
                disconnectList.add(String.valueOf(channelID));
            }
        }
    }

    public static Schema[] schemaVersions() {
        return new Schema[] {
                RELOAD_REQUEST_SCHEMA_V0, RELOAD_REQUEST_SCHEMA_V1
        };
    }
    @Override
    protected Struct toStruct() {
        Struct requestStruct = new Struct(ApiKeys.RELOAD_AUTH_META.requestSchema(version()));
        requestStruct.set(REPLICA_ID_KEY_NAME, replicaID);
        requestStruct.set(DISCONNECT_STRATEGY, disconnectStrategy);
        if (null != disconnectList) {
            requestStruct.set(DISCONNECT_LIST, disconnectList.toArray());
        }
        return requestStruct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        String errMessage = e.getMessage();
        return new ReloadAuthMetaResponse(replicaID, false, false, null, errMessage);
    }

    public static ReloadAuthMetaRequest parse(ByteBuffer buffer, short version) {
        return new ReloadAuthMetaRequest(ApiKeys.RELOAD_AUTH_META.parseRequest(version, buffer), version);
    }

    public static class Builder extends AbstractRequest.Builder<ReloadAuthMetaRequest> {
        private int replicaID;
        private int disconnectStrategy;
        private List<String> disconnectList;

        public Builder(int replicaID, DisconnectStrategy disconnectStrategy, List<String> disconnectList) {
            super(ApiKeys.RELOAD_AUTH_META);
            this.replicaID = replicaID;
            this.disconnectStrategy = disconnectStrategy.getStrategy();
            this.disconnectList = disconnectList;
        }

        @Override
        public ReloadAuthMetaRequest build(short version) {
            return new ReloadAuthMetaRequest(replicaID, disconnectStrategy, disconnectList, version);
        }
    }
}
