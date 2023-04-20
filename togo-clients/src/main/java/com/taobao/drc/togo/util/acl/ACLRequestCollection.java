package com.taobao.drc.togo.util.acl;

import org.apache.kafka.common.protocol.ApiKeys;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by longxuan on 17/12/1.
 */
public class ACLRequestCollection {
    private Set<Short> allowOperationRequestID;
    private boolean isSuperUser;

    private ACLRequestCollection(Set<Short> apiKeyIDs) {
        this.allowOperationRequestID = apiKeyIDs;
        isSuperUser = false;
    }

    private ACLRequestCollection() {
        allowOperationRequestID = new HashSet<>();
        isSuperUser = false;
    }

    public ACLRequestCollection append(short apiKeyID) {
        allowOperationRequestID.add(apiKeyID);
        return this;
    }

    public ACLRequestCollection append(ApiKeys apiKey) {
        allowOperationRequestID.add(apiKey.id);
        return this;
    }

    public ACLRequestCollection append(Set<Short> apiKeyIDs) {
        allowOperationRequestID.addAll(apiKeyIDs);
        return this;
    }

    public ACLRequestCollection superPrivilege() {
        isSuperUser = true;
        return this;
    }

    public boolean requestAuthorized(short id) {
        if (isSuperUser) {
            return true;
        } else {
            return allowOperationRequestID.contains(id);
        }
    }

    public ACLRequestCollection copy() {
        ACLRequestCollection aclRequestCollection = new ACLRequestCollection();
        aclRequestCollection.allowOperationRequestID.addAll(this.allowOperationRequestID);
        aclRequestCollection.isSuperUser = this.isSuperUser;
        return aclRequestCollection;
    }

    public static ACLRequestCollection SUPER_PRIVILEGES = ACLRequestCollectionBuilder.create().superPrivilege();
    public static ACLRequestCollection DEFAULT_READ_PRIVILEGES = ACLRequestCollectionBuilder.create()
            .append(ApiKeys.FETCH)
            .append(ApiKeys.API_VERSIONS)
            .append(ApiKeys.METADATA)
            .append(ApiKeys.LIST_OFFSETS)
            .append(ApiKeys.OFFSET_COMMIT)
            .append(ApiKeys.OFFSET_FETCH)
            .append(ApiKeys.FIND_COORDINATOR)
            .append(ApiKeys.JOIN_GROUP)
            .append(ApiKeys.HEARTBEAT)
            .append(ApiKeys.LEAVE_GROUP)
            .append(ApiKeys.SYNC_GROUP)
            .append(ApiKeys.DESCRIBE_GROUPS)
            .append(ApiKeys.LIST_GROUPS)
            .append(ApiKeys.SASL_AUTHENTICATE)
            .append(ApiKeys.SASL_HANDSHAKE)
            .append(ApiKeys.CREATE_RECORD_METAS)
            .append(ApiKeys.FLEX_LIST_OFFSET)
            .append(ApiKeys.FETCH_RECORD_METAS)
            .append(ApiKeys.SET_FETCH_TAGS);
    public static ACLRequestCollection DEFAULT_WRITE_PRIVILEGES = SUPER_PRIVILEGES;
    public static ACLRequestCollection DEFAULT_INVALID_PRIVILEGES = ACLRequestCollectionBuilder.create();
    public static class ACLRequestCollectionBuilder {
        public static ACLRequestCollection create() {
            return new ACLRequestCollection();
        }
        public static ACLRequestCollection create(Set<ApiKeys> apiKeys) {
            Set<Short> apiKeyIDs = new HashSet<>();
            apiKeys.forEach((apiKey)->{
                apiKeyIDs.add(apiKey.id);
            });
            return new ACLRequestCollection(apiKeyIDs);
        }
    }
}
