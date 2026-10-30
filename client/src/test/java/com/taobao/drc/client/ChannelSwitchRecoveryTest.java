package com.taobao.drc.client;

import com.sun.net.httpserver.HttpServer;
import com.taobao.drc.client.impl.*;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.store.StoreClient;
import org.junit.Test;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.Assert.*;

public class ChannelSwitchRecoveryTest {
    static class Client extends DRCClientImpl {
        Client(Properties props) { super(props); }
        StoreClient select(boolean multi) { return super.switchClient(multi); }
        UserConfig config() throws Exception {
            Field field = DRCClientImpl.class.getDeclaredField("userConfig"); field.setAccessible(true);
            return (UserConfig) field.get(this);
        }
    }
    @Test public void switchingEitherDirectionUsesImmutableOriginalTimestamp() throws Exception {
        for (String source : new String[] {"drc", "dts"}) {
            for (boolean multi : new boolean[] {false, true}) { verifySwitch(source, multi, false); }
        }
    }
    @Test public void positionOnlySwitchFailsWithoutInventingATimestamp() throws Exception {
        verifySwitch("drc", false, true);
    }
    private void verifySwitch(String source, boolean multi, boolean positionOnly) throws Exception {
        AtomicReference<String> type = new AtomicReference<>(source);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/client/switch/", exchange -> {
            String data = exchange.getRequestURI().getPath().endsWith("verify")
                    ? "{\"taskType\":\"" + type.get() + "\"}" : "{\"serverAddr\":\"test\",\"regionId\":\"test\"}";
            byte[] body = ("{\"isSuccess\":true,\"data\":" + data + "}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length); exchange.getResponseBody().write(body); exchange.close();
        });
        server.start(); Client client = null;
        try {
            Properties props = new Properties(); props.setProperty("manager.host", "http://127.0.0.1:" + server.getAddress().getPort());
            client = new Client(props); client.addDataFilter(new DataFilter("*;*;*"));
            Checkpoint supplied = new Checkpoint();
            if (positionOnly) { supplied.setPosition("4@mysql-bin.000001"); } else { supplied.setTimestamp("1000"); }
            client.initService("test", "test", "test", supplied, null);
            supplied.setTimestamp("8000");
            StoreClient original = client.select(multi);
            client.config().getCheckpoint().setTimestamp("9000");
            client.config().getCheckpoint().setOffset(99L);
            assertSame(original, client.select(multi));
            type.set(source.equals("drc") ? "dts" : "drc");
            try {
                StoreClient replacement = client.select(multi);
                if (positionOnly) { fail("No portable original timestamp"); }
                assertNotSame(original, replacement);
                assertEquals("1000", client.config().getCheckpoint().getTimestamp());
                assertNull(client.config().getCheckpoint().getPosition());
                assertNull(client.config().getCheckpoint().getOffset());
            } catch (IllegalStateException expected) {
                if (!positionOnly) { throw expected; }
            }
        } finally { if (client != null) client.stopService(); server.stop(0); }
    }
}
