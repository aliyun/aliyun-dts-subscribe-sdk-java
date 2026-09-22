package com.taobao.drc.client.network.dstore;

import com.sun.net.httpserver.HttpServer;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.togo.client.consumer.SchemafulConsumerRecords;
import com.taobao.drc.togo.client.consumer.TogoConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SubscriptionConsumerIdleTest {
    private HttpServer cm;
    private final AtomicInteger requests = new AtomicInteger();
    private String channel = "dts";
    private SubscriptionConsumer consumer;
    private TogoConsumer transport;

    @Before public void setup() throws Exception {
        cm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        cm.createContext("/client/switch/verify", exchange -> {
            requests.incrementAndGet();
            byte[] body = ("{\"isSuccess\":true,\"data\":{\"taskType\":\"" + channel + "\"}}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        cm.start();
        UserConfig config = RecoveryPositionTest.config();
        config.setClusterUrl("http://127.0.0.1:" + cm.getAddress().getPort());
        config.setDb("test");
        config.setUserName("test");
        consumer = new SubscriptionConsumer(null);
        consumer.config = config;
        transport = mock(TogoConsumer.class);
        when(transport.poll(anyLong())).thenReturn(new SchemafulConsumerRecords(Collections.emptyMap()));
        set("consumer", transport);
        set("socketTimeOut", 60000L);
    }
    @After public void teardown() { cm.stop(0); }
    private void set(String name, Object value) throws Exception {
        Field field = SubscriptionConsumer.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(consumer, value);
    }

    private void assertEmptyWithoutReset() throws Exception {
        try {
            assertTrue(consumer.poll(1).isEmpty());
        } finally {
            verify(transport, never()).close();
        }
    }

    @Test public void firstEmptyPollDoesNotImmediatelyCheckOrReset() throws Exception {
        assertEmptyWithoutReset();
        assertEquals(0, requests.get());
        verify(transport, never()).close();
    }

    @Test public void idleDtsPollChecksChannelButPreservesTransportAndThrottlesChecks() throws Exception {
        set("lastReceivedTime", 1L);
        assertEmptyWithoutReset();
        assertEmptyWithoutReset();
        assertEquals(1, requests.get());
        verify(transport, never()).close();
        verify(transport, times(2)).poll(1L);
    }

    @Test(expected = DStoreSwitchException.class)
    public void idlePollStillRequestsSwitchWhenCmReturnsDrc() throws Exception {
        channel = "drc";
        set("lastReceivedTime", 1L);
        consumer.poll(1);
    }
}
