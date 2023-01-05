package com.taobao.drc.client.utils;

import com.taobao.drc.client.DRCClientException;
import com.taobao.drc.client.cm.ClusterManagerFacade;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.DBType;

import java.io.IOException;

public class NetworkUtils {

    public static DBType retrieveDBTypeFromRemote(UserConfig userConfig, String subTopic) throws IOException, DRCClientException {
        return ClusterManagerFacade.askTopicType(userConfig, subTopic);
    }

}
