package com.taobao.drc.client;

import org.junit.Test;

import java.io.IOException;
import java.util.Properties;
import static org.junit.Assert.*;

public class DRCClientFactoryTest {

    @Test
    public void test() {
        Properties pro = new Properties();
        pro.put("hello", "world");
        try {
            DRCClient clientMysql = DRCClientFactory.create(DRCClientFactory.Type.MYSQL, pro);
            DRCClient clientOceanBase = DRCClientFactory.create(DRCClientFactory.Type.OCEANBASE, pro);
            assertTrue(true);
        } catch (IOException e) {
            assertTrue(false);
        }

        try {
            DRCClient clientInvalid = DRCClientFactory.create(DRCClientFactory.Type.NONE, pro);
            assertTrue(clientInvalid == null);
            int i = 10;
            DRCClient clientInvalid2 = DRCClientFactory.create(DRCClientFactory.Type.NONE, i);
            assertTrue(clientInvalid2 == null);
        } catch (IOException e) {
            assertTrue(false);
        }

        try {
            DRCClient clientInvalid = DRCClientFactory.create(pro);
            int i = 10;
            DRCClient clientInvalid2 = DRCClientFactory.create(i);
            assertTrue(clientInvalid2 == null);
        } catch (IOException e) {
            assertTrue(false);
        }

    }
}
