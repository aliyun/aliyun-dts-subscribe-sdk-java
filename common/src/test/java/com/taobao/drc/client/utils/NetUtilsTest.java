package com.taobao.drc.client.utils;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class NetUtilsTest {

    @Test
    public void test() {
        String localIp = "127.0.0.1";
        String invalidIp = "256.23.23424.23";
        String normalIP = "10.232.31.134";
        String zeroIP = "0.0.0.0";
        /*
         *  ip to long miss the valid judgement of ip
         */
        long longLocalIp = NetUtils.ipToLong(localIp);
        assertTrue(longLocalIp == ((127 << 24) | 1));


    }
}
