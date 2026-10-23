package com.taobao.drc.client.store.impl;

import com.sun.net.httpserver.HttpServer;
import com.taobao.drc.client.SubscribeChannel;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.network.DStoreNetworkEndpoint;
import com.taobao.drc.client.network.RecordNotifyHelper;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.enums.DBType;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DStoreRegistrationPositionTest {
    static class Client extends DStoreClientImpl {
        Client(UserConfig config, CheckpointManager manager) {
            super(null, config, manager, null, null, null);
            userConfigMap.put("test-0", config);
            checkpointManagerMap.put("test-0", manager);
        }
    }

    @Test public void multiServiceRegistersConfirmedPositionBeforeCreatingConsumer() throws Exception {
        assertRegistration(true);
    }
    @Test public void singleServiceRegistrationUsesSameRecoverySelection() throws Exception {
        assertRegistration(false);
    }
    private void assertRegistration(boolean multiService) throws Exception {
        final AtomicReference<String> form = new AtomicReference<String>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/client/switch/", exchange -> {
            String response;
            if (exchange.getRequestURI().getPath().endsWith("register")) {
                java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int n;
                while ((n = exchange.getRequestBody().read(buffer)) != -1) bytes.write(buffer, 0, n);
                form.set(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
                response = "{\"isSuccess\":true,\"data\":\"channel-test\"}";
            } else {
                response = "{\"isSuccess\":true,\"data\":{\"topics\":[\"test-0\"],\"regionId\":\"test\"}}";
            }
            byte[] body = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        Client client = null;
        try {
            UserConfig config = new UserConfig();
            Checkpoint checkpoint = new Checkpoint();
            checkpoint.setTimestamp("1000");
            config.setCheckpoint(checkpoint);
            config.setDb("test");
            config.setUserName("test");
            config.setSubTopic("test-0");
            config.setSubscribeChannel(SubscribeChannel.DTS);
            config.setClusterUrl("http://127.0.0.1:" + server.getAddress().getPort());
            CheckpointManager manager = new CheckpointManager(multiService);
            Listener listener = mock(Listener.class);
            doAnswer(invocation -> {
                DataMessage message = (DataMessage) invocation.getArguments()[0];
                for (DataMessage.Record record : message.getRecordList()) {
                    record.ackAsConsumed();
                }
                return null;
            }).when(listener).notify(any(DataMessage.class));
            RecordNotifyHelper helper = new RecordNotifyHelper(config, manager.beginRecovery("1000"), listener);
            helper.process(new DataMessage.Record() {
                @Override public Type getOpt() { return Type.INSERT; }
                @Override public String getTimestamp() { return "1200"; }
                @Override public DBType getDbType() { return DBType.MYSQL; }
            });
            client = new Client(config, manager);
            Field endpoint = DStoreClientImpl.class.getDeclaredField("endpoint");
            endpoint.setAccessible(true);
            endpoint.set(client, mock(DStoreNetworkEndpoint.class));
            assertTrue((multiService ? client.startMultiService() : client.startService()).call());
            assertNotNull(form.get());
            assertTrue(form.get(), form.get().contains("checkpoint=1200"));
            assertEquals("1200", manager.beginRecovery("1000").getTimestamp());
            assertEquals("1000", checkpoint.getTimestamp());
        } finally {
            if (client != null) client.stopService();
            server.stop(0);
        }
    }
}
