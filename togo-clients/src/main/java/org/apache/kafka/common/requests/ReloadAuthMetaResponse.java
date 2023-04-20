package org.apache.kafka.common.requests;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.*;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by longxuan on 18/2/23.
 */
public class ReloadAuthMetaResponse extends AbstractResponse {
    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String RELOADED = "reloaded";
    private static final String DISCONNECTED = "disconnected";
    private static final String RELOADED_DESCRIBE = "reloadDescribe";
    private static final String ERR_MESSAGE = "errMessage";
    private static final Schema RELOAD_AUTH_META_RESPONSE_SCHEMA_V0 = new Schema(new Field(REPLICA_ID_KEY_NAME, Type.INT32),
            new Field(RELOADED, Type.BOOLEAN, "if reload is done"),
            new Field(DISCONNECTED, Type.BOOLEAN, "if consumer is disconnected"),
            new Field(RELOADED_DESCRIBE, ArrayOf.nullable(Type.STRING), "reload ret string"),
            new Field(ERR_MESSAGE, Type.NULLABLE_STRING, "err message if have"));
    private static final Schema RELOAD_AUTH_META_RESPONSE_SCHEMA_V1 = new Schema(new Field(REPLICA_ID_KEY_NAME, Type.INT32),
            new Field(RELOADED, Type.BOOLEAN, "if reload is done"),
            new Field(DISCONNECTED, Type.BOOLEAN, "if consumer is disconnected"),
            new Field(ERR_MESSAGE, Type.NULLABLE_STRING, "err message if have"));


    public static Schema[] schemaVersions() {
        return new Schema[]{
                RELOAD_AUTH_META_RESPONSE_SCHEMA_V0, RELOAD_AUTH_META_RESPONSE_SCHEMA_V1
        };
    }

    private int replicaID;
    private boolean reloaded;
    private boolean disconnected;
    private List<String> reloadDescribe;
    private String errMessage;

    public int getReplicaID() {
        return replicaID;
    }

    public boolean isReloaded() {
        return reloaded;
    }

    public boolean isDisconnected() {
        return disconnected;
    }

    public String getErrMessage() {
        return errMessage;
    }

    public List<String> getReloadDescribe() {
        return reloadDescribe;
    }

    public ReloadAuthMetaResponse(int replicaID, boolean reloaded, boolean disconnected, List<String> reloadDescribe, String errMessage) {
        this.replicaID = replicaID;
        this.reloaded = reloaded;
        this.disconnected = disconnected;
        this.errMessage = errMessage;
        this.reloadDescribe = reloadDescribe;
    }

    public ReloadAuthMetaResponse(Struct struct) {
        this.replicaID = struct.getInt(REPLICA_ID_KEY_NAME);
        this.reloaded = struct.getBoolean(RELOADED);
        this.disconnected = struct.getBoolean(DISCONNECTED);
        this.errMessage = struct.getString(ERR_MESSAGE);

        if (struct.hasField(RELOADED_DESCRIBE)) {
            Object[] reloadDescribeStruct = struct.getArray(RELOADED_DESCRIBE);
            if (null != reloadDescribeStruct) {
                reloadDescribe = new LinkedList<>();
                for (Object object : reloadDescribeStruct) {
                    reloadDescribe.add((String) object);
                }
            }
        }
    }


    @Override
    public Map<Errors, Integer> errorCounts() {
        return null;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.RELOAD_AUTH_META.responseSchema(version));
        struct.set(REPLICA_ID_KEY_NAME, replicaID);
        struct.set(RELOADED, reloaded);
        struct.set(DISCONNECTED, disconnected);
        if (null != errMessage) {
            struct.set(ERR_MESSAGE, errMessage);
        }

        if (struct.hasField(RELOADED_DESCRIBE) && null != reloadDescribe) {
            struct.set(RELOADED_DESCRIBE, reloadDescribe.toArray());
        }
        return struct;
    }

    public static ReloadAuthMetaResponse parse(ByteBuffer buffer) {
        return new ReloadAuthMetaResponse(ApiKeys.RELOAD_AUTH_META.responseSchema(ApiKeys.RELOAD_AUTH_META.latestVersion()).read(buffer));
    }

    public static ReloadAuthMetaResponse parse(ByteBuffer buffer, int version) {
        return new ReloadAuthMetaResponse(ApiKeys.RELOAD_AUTH_META.responseSchema((short)version).read(buffer));
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{").append(REPLICA_ID_KEY_NAME).append(":").append(replicaID)
                .append(",").append(RELOADED).append(":").append(reloaded)
                .append(",").append(DISCONNECTED).append(":").append(disconnected)
                .append(",").append(RELOADED_DESCRIBE).append(":").append(null == reloadDescribe ? "" : StringUtils.join(reloadDescribe, ","))
                .append(",").append(ERR_MESSAGE).append(":").append(errMessage).append("}");
        return stringBuilder.toString();
    }

    public int throttleTimeMs() {
        return 0;
    }
}
