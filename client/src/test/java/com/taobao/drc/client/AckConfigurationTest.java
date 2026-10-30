package com.taobao.drc.client;

import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.impl.DRCClientImpl;
import org.junit.Test;
import java.lang.reflect.Field;
import java.util.Properties;
import static org.junit.Assert.*;

public class AckConfigurationTest {
    private UserConfig initialize(String maximum) throws Exception {
        Properties properties = new Properties();
        if (maximum != null) properties.setProperty("client.maxPendingAckRanges", maximum);
        DRCClient client = DRCClientFactory.create(properties);
        client.addDataFilter(new DataFilter("*;*;*"));
        assertTrue(client.getCheckpointStatus().isEmpty());
        client.initService("test", "test", "test", "1000", null);
        Field config = DRCClientImpl.class.getDeclaredField("userConfig");
        config.setAccessible(true);
        return (UserConfig) config.get(client);
    }

    @Test public void defaultDoesNotLimitLegacyConsumers() throws Exception {
        assertEquals(0, initialize(null).getMaxPendingAckRanges());
    }
    @Test public void explicitRangeBudgetIsLoaded() throws Exception {
        assertEquals(256, initialize("256").getMaxPendingAckRanges());
    }
    @Test(expected = IllegalArgumentException.class)
    public void negativeBudgetIsRejectedDuringInitialization() throws Exception { initialize("-1"); }
}
