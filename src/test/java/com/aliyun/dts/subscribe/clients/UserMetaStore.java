package com.aliyun.dts.subscribe.clients;

import com.aliyun.dts.subscribe.clients.metastore.AbstractUserMetaStore;

/**
 * store the checkpoint data in the shared storage, such us database, shared file storage...
 * this meta store need to be completed by consumer
 */
public class UserMetaStore extends AbstractUserMetaStore {

    @Override
    protected void saveData(String groupID, String toStoreJson) {

    }

    @Override
    protected String getData(String groupID) {
        return null;
    }
}
