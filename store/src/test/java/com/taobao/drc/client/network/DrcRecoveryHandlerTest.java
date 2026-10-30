package com.taobao.drc.client.network;

import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.network.congestion.CongestionController;
import com.taobao.drc.client.utils.Constant;
import com.taobao.drc.client.utils.NetworkConstant;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class DrcRecoveryHandlerTest {
    private static class ListenerStub implements Listener {
        final List<DataMessage.Record> records = new ArrayList<DataMessage.Record>();
        public void notify(DataMessage message) { records.addAll(message.getRecordList()); }
        public void notifyRuntimeLog(String level, String text) { }
        public void handleException(Exception error) { }
    }
    private static EmbeddedChannel channel(CheckpointManager owner, Listener listener) {
        UserConfig config = new UserConfig(); config.setCheckpoint(new Checkpoint());
        config.getCheckpoint().setTimestamp("1000"); config.setSubTopic("test-0");
        config.setMultiMode(owner.isMultiMode());
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(Constant.configKey).set(config);
        channel.attr(Constant.channelSetKey).set(new HashSet<Channel>());
        channel.attr(NetworkConstant.listenerKey).set(listener);
        channel.attr(NetworkConstant.checkpointManagerKey).set(owner);
        channel.attr(ConnectionStateChangeListener.CONNECTION_STATE_CHANGE_LISTENER_ATTRIBUTE_KEY).set(new ConnectionStateChangeListener() {
            public void onChannelActive(Channel channel) { }
            public void onFirstRecord(Channel channel) { }
            public long onChannelInactive(Channel channel) { return -1; }
            public long onException(Channel channel, Throwable error) { return -1; }
        });
        channel.pipeline().addLast(new RecordNotifyHandler(new CongestionController() {
            public boolean produced() { return false; }
            public boolean consumed() { return false; }
            public long pendingNum() { return 0; }
        }));
        channel.pipeline().fireChannelActive();
        return channel;
    }
    private static DataMessage.Record record(DataMessage.Record.Type type, String timestamp) {
        DataMessage.Record r = new DataMessage.Record(); r.setType(type);
        r.addAttribute("timestamp", timestamp); r.addAttribute("source_type", "mysql"); return r;
    }

    @Test public void hashRecoveryWaitsForAckedHeartbeatAndDoesNotRetainLinks() {
        CheckpointManager owner = new CheckpointManager(true); ListenerStub listener = new ListenerStub();
        EmbeddedChannel channel = channel(owner, listener);
        try {
            channel.writeInbound(record(DataMessage.Record.Type.INSERT, "1200"));
            channel.writeInbound(record(DataMessage.Record.Type.HEARTBEAT, "1100"));
            listener.records.get(1).ackAsConsumed();
            assertEquals("1000", owner.getDrcRecoveryTimestamp("1000"));
            listener.records.get(0).ackAsConsumed();
            assertEquals("1100", owner.getDrcRecoveryTimestamp("1000"));
            assertNull(listener.records.get(0).getPrev());
        } finally { channel.close(); }
    }

    @Test public void oldConnectionCannotDeliverAfterReplacementStarts() {
        CheckpointManager owner = new CheckpointManager(false); ListenerStub listener = new ListenerStub();
        EmbeddedChannel first = channel(owner, listener), second = channel(owner, listener);
        try {
            first.writeInbound(record(DataMessage.Record.Type.HEARTBEAT, "9000"));
            second.writeInbound(record(DataMessage.Record.Type.HEARTBEAT, "1200"));
            assertEquals(1, listener.records.size());
            assertEquals("1200", owner.getDrcRecoveryTimestamp("1000"));
        } finally { first.close(); second.close(); }
    }

    @Test public void asyncAckOverflowClosesIdleDrcConnection() throws Exception {
        CheckpointManager owner = new CheckpointManager(true); owner.setMaxPendingAckRanges(1);
        ListenerStub listener = new ListenerStub(); EmbeddedChannel channel = channel(owner, listener);
        try {
            for (int i = 0; i < 4; i++) { channel.writeInbound(record(DataMessage.Record.Type.INSERT, "1100")); }
            listener.records.get(1).ackAsConsumed();
            java.util.concurrent.ExecutorService business = java.util.concurrent.Executors.newSingleThreadExecutor();
            try {
                try { business.submit(() -> listener.records.get(3).ackAsConsumed()).get(2, java.util.concurrent.TimeUnit.SECONDS);
                    fail("Expected ACK budget failure"); }
                catch (java.util.concurrent.ExecutionException expected) { assertSame(owner.getFailure(), expected.getCause()); }
            } finally { business.shutdownNow(); }
            channel.runPendingTasks();
            assertFalse(channel.isOpen());
            assertNotNull(owner.getFailure());
        } finally { channel.close(); }
    }
}
