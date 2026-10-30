package com.taobao.drc.client.store.impl;

import com.sun.net.httpserver.HttpServer;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.network.DStoreNetworkEndpoint;
import org.junit.Test;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TopologyRecoveryTest {
    @Test public void addedRemovedAndReplacedShardsRestartFromOriginalTimestamp() throws Exception {
        for (String[] changed : new String[][] {{"a", "b", "c"}, {"b"}, {"c", "d"}}) { verifyTopology(changed); }
    }
    private void verifyTopology(String[] changed) throws Exception {
        AtomicReference<String[]> topics = new AtomicReference<>(new String[] {"a", "b"});
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/client/switch/", exchange -> {
            String data = exchange.getRequestURI().getPath().endsWith("register") ? "\"test-channel\""
                    : "{\"topics\":[\"" + String.join("\",\"", topics.get()) + "\"],\"regionId\":\"test\"}";
            byte[] body = ("{\"isSuccess\":true,\"data\":" + data + "}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length); exchange.getResponseBody().write(body); exchange.close();
        });
        server.start(); DStoreClientImpl client = null;
        try {
            UserConfig config = new UserConfig(); config.setMultiMode(true); config.setCheckpoint(new Checkpoint());
            config.setSubscribeChannel(com.taobao.drc.client.SubscribeChannel.DTS);
            config.getCheckpoint().setTimestamp("1000"); config.setDb("test"); config.setUserName("test");
            config.setClusterUrl("http://127.0.0.1:" + server.getAddress().getPort());
            client = new DStoreClientImpl(null, config, new CheckpointManager(true), null, null, null);
            DStoreNetworkEndpoint endpoint = mock(DStoreNetworkEndpoint.class);
            Field field = DStoreClientImpl.class.getDeclaredField("endpoint"); field.setAccessible(true); field.set(client, endpoint);
            client.startMultiService().call();
            for (CheckpointManager manager : client.checkpointManagerMap.values()) { manager.setSaveCheckpoint("1200"); }
            client.startMultiService().call();
            assertEquals("1200", client.getMultiSafeTimestamp());
            config.getCheckpoint().setTimestamp("9000"); // mutable diagnostic value is not the original start
            topics.set(changed);
            client.startMultiService().call();
            assertEquals(new HashSet<>(Arrays.asList(changed)), client.getCheckpointStatus().keySet());
            assertNull("New topology cannot aggregate only the previously known shards", client.getMultiSafeTimestamp());
            for (CheckpointManager manager : client.checkpointManagerMap.values()) {
                assertEquals("1000", manager.getRecoveryTimestamp("9000"));
            }
        } finally { if (client != null) client.stopService(); server.stop(0); }
    }
}
