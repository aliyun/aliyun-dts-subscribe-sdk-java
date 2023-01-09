package com.taobao.drc.client.network;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AutoReadAwareReadTimeoutHandlerTest {
    @Test
    public void testTimeout() throws Exception {
        int readTimeoutSeconds = 5;
        EmbeddedChannel channel = new EmbeddedChannel(new AutoReadAwareReadTimeoutHandler(readTimeoutSeconds));
        channel.read();
        for (int i = 0; i < readTimeoutSeconds; i++) {
            channel.runPendingTasks();
            assertTrue(channel.isActive());
            TimeUnit.SECONDS.sleep(1);
        }
        TimeUnit.SECONDS.sleep(AutoReadAwareReadTimeoutHandler.DEFAULT_READER_IDLE_TIME_SECONDS - (readTimeoutSeconds % AutoReadAwareReadTimeoutHandler.DEFAULT_READER_IDLE_TIME_SECONDS));

        channel.runPendingTasks();;
        assertFalse(channel.isActive());
    }

    @Test
    public void testNormalRead() throws Exception {
        int readTimeoutSeconds = 5;
        EmbeddedChannel channel = new EmbeddedChannel(new AutoReadAwareReadTimeoutHandler(readTimeoutSeconds));

        for (int i = 0; i < readTimeoutSeconds; i++) {
            channel.runPendingTasks();
            assertTrue(channel.isActive());
            channel.read();
            channel.writeInbound(new Object());
            TimeUnit.SECONDS.sleep(1);
        }
        TimeUnit.SECONDS.sleep(AutoReadAwareReadTimeoutHandler.DEFAULT_READER_IDLE_TIME_SECONDS - (readTimeoutSeconds % AutoReadAwareReadTimeoutHandler.DEFAULT_READER_IDLE_TIME_SECONDS));

        channel.runPendingTasks();;
        assertTrue(channel.isActive());
    }

    @Test
    public void testNoAutoRead() throws Exception {
        int readTimeoutSeconds = 5;
        EmbeddedChannel channel = new EmbeddedChannel(new AutoReadAwareReadTimeoutHandler(readTimeoutSeconds));
        // make read complete
        channel.config().setAutoRead(false);
        channel.writeInbound(new Object());

        for (int i = 0; i < readTimeoutSeconds; i++) {
            channel.runPendingTasks();
            assertTrue(channel.isActive());
            TimeUnit.SECONDS.sleep(1);
        }
        TimeUnit.SECONDS.sleep(AutoReadAwareReadTimeoutHandler.DEFAULT_READER_IDLE_TIME_SECONDS - (readTimeoutSeconds % AutoReadAwareReadTimeoutHandler.DEFAULT_READER_IDLE_TIME_SECONDS));

        channel.runPendingTasks();;
        assertTrue(channel.isActive());

        //
        channel.read();
        TimeUnit.SECONDS.sleep(readTimeoutSeconds + AutoReadAwareReadTimeoutHandler.DEFAULT_READER_IDLE_TIME_SECONDS - (readTimeoutSeconds % AutoReadAwareReadTimeoutHandler.DEFAULT_READER_IDLE_TIME_SECONDS));

        channel.runPendingTasks();;
        assertFalse(channel.isActive());
    }
}
