package com.taobao.drc.togo.util;


import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by longxuan on 2018/10/30.
 * limit the connection number for each sid. default is Integer.MAX
 */
public class SIDQuotas {
    private static final Logger logger = LoggerFactory.getLogger(com.taobao.drc.togo.util.SIDQuotas.class);

    private final HashMap<String, Integer> SIDQuotas;
    private final HashMap<String, Integer> limitedSID;

    public SIDQuotas() {
        this.SIDQuotas = new HashMap<>();
        this.limitedSID = new HashMap<>();
    }

    public void inc(String SID) {
        synchronized(this) {
            Integer maxQuotaForSID = limitedSID.get(SID);
            if (null == maxQuotaForSID || maxQuotaForSID <= 0) {
                return;
            }
            Integer lastQuota = SIDQuotas.get(SID);
            if (null == lastQuota) {
                SIDQuotas.put(SID, 1);
            } else {
                if (lastQuota >= maxQuotaForSID) {
                    logger.error("SIDQuotas: current connection number is " + lastQuota + ", limit is " + maxQuotaForSID + ", too many connection for  " + SID);
                    throw new KafkaException("SIDQuotas: current connection number is " + lastQuota + ", limit is " + maxQuotaForSID + ", too many connection for  " + SID);
                }
                logger.info("SIDQuotas: SID [" + SID + "] accept new connection, current connected connection num [" + lastQuota + 1 + "]");
                SIDQuotas.put(SID, lastQuota + 1);
            }
        }
    }

    public void dec(String SID) {
        synchronized (this) {
            Integer lastQuota = SIDQuotas.get(SID);
            if (null == lastQuota) {
                logger.warn("SIDQuotas: get no quota info for SID [" + SID + "]");
            } else {
                lastQuota = lastQuota - 1;
                if (lastQuota < 0) {
                    logger.warn("SIDQuotas: quota is negative for SID [" + SID + "], reset it");
                    lastQuota = 0;
                }
                logger.info("SID: [" + SID + "] disconnect connection, current connected connection num [" + lastQuota + "]");
                SIDQuotas.put(SID, lastQuota);
            }
        }
    }

    public Integer getMaxConnectionForSID(String SID) {
        Integer quotaForSID = limitedSID.get(SID);
        return (quotaForSID == null || quotaForSID <= 0) ? Integer.MAX_VALUE : quotaForSID;
    }
    // for test
    public Integer getConnectionStateForSID(String SID) {
        synchronized (this) {
            Integer ret = SIDQuotas.get(SID);
            return null == ret ? 0 : ret;
        }
    }

    public void setQuotaLimitForSID(String SID, Integer quota) {
        synchronized (this) {
            if (quota == null || quota <= 0) {
                return;
            }
            limitedSID.put(SID, quota);
        }
    }
}
