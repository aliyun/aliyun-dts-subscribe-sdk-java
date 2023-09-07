package com.taobao.drc.client.network.congestion;

import com.taobao.drc.client.config.UserConfig;

/**
 * @author yangyang
 * @since 17/1/19
 */
public class CongestionControllers {
    public static CongestionController createFromConfig(UserConfig userConfig) {
        long maxPendingRecords = userConfig.getMaxPendingRecords();
        return new ThresholdBasedCongestionController(maxPendingRecords / 2, maxPendingRecords);
    }
}
