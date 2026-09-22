package com.taobao.drc.client.network.dstore;

import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.impl.RecordsCache;
import com.taobao.drc.client.message.DataMessage;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class RecoveryPositionTest {
    static class CapturingListener implements Listener {
        final List<DataMessage.Record> delivered = new ArrayList<DataMessage.Record>();
        public void notify(DataMessage message) { delivered.addAll(message.getRecordList()); }
        public void notifyRuntimeLog(String level, String message) { }
        public void handleException(Exception e) { throw new AssertionError(e); }
    }

    static class TestConsumer extends BaseDStoreConsumer {
        long requestedTimestamp;
        boolean missing;
        TestConsumer(Listener listener) {
            super(listener);
            assignedPartition = new TopicPartition("test-0", 0);
        }
        void receive(final DataMessage.Record.Type operation, final String timestamp) throws Exception {
            notifyHeper.process(new DataMessage.Record() {
                @Override public Type getOpt() { return operation; }
                @Override public String getTimestamp() { return timestamp; }
                @Override public DBType getDbType() { return DBType.MYSQL; }
            });
        }
        List<DataMessage.Record> poll(long timeout) { return Collections.emptyList(); }
        void doClose() { }
        Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
            return Collections.singletonMap(assignedPartition, 0L);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestamps) {
            requestedTimestamp = timestamps.get(assignedPartition);
            return Collections.singletonMap(assignedPartition,
                    missing ? null : new OffsetAndTimestamp(42L, requestedTimestamp));
        }
    }

    static UserConfig config() {
        UserConfig config = new UserConfig();
        Checkpoint checkpoint = new Checkpoint();
        checkpoint.setTimestamp("1000");
        config.setCheckpoint(checkpoint);
        config.setPollTimeoutMs("200");
        return config;
    }

    @Test public void recoversFromAcknowledgedTransactionSafeTimestamp() throws Exception {
        UserConfig config = config();
        CheckpointManager manager = new CheckpointManager(true);
        CapturingListener listener = new CapturingListener();
        TestConsumer first = new TestConsumer(listener);
        first.init(config, manager);
        first.receive(DataMessage.Record.Type.BEGIN, "1100");
        first.receive(DataMessage.Record.Type.INSERT, "1200");
        listener.delivered.get(0).ackAsConsumed();
        listener.delivered.get(1).ackAsConsumed();
        TestConsumer recovered = new TestConsumer(listener);
        recovered.init(config, manager);
        assertEquals(42L, recovered.getPosition());
        assertEquals(1100L, recovered.requestedTimestamp);
    }

    @Test public void noAckUsesOriginalStartEvenAfterHeartbeatAdvancedConfig() throws Exception {
        UserConfig config = config();
        CheckpointManager manager = new CheckpointManager(true);
        CapturingListener listener = new CapturingListener();
        TestConsumer first = new TestConsumer(listener);
        first.init(config, manager);
        first.receive(DataMessage.Record.Type.INSERT, "1200");
        first.receive(DataMessage.Record.Type.HEARTBEAT, "9000");
        TestConsumer recovered = new TestConsumer(listener);
        recovered.init(config, manager);
        recovered.getPosition();
        assertEquals(1000L, recovered.requestedTimestamp);
    }

    @Test public void lateOldAckCannotAdvanceNewGenerationOrBlockReplayAck() throws Exception {
        UserConfig config = config();
        CheckpointManager manager = new CheckpointManager(true);
        CapturingListener listener = new CapturingListener();
        TestConsumer first = new TestConsumer(listener);
        first.init(config, manager);
        first.receive(DataMessage.Record.Type.INSERT, "1100");
        listener.delivered.get(0).ackAsConsumed();
        first.receive(DataMessage.Record.Type.INSERT, "1800");
        first.receive(DataMessage.Record.Type.INSERT, "1900");
        TestConsumer recovered = new TestConsumer(listener);
        recovered.init(config, manager);
        listener.delivered.get(1).ackAsConsumed();
        assertEquals("1100", manager.getSaveCheckpoint());
        recovered.receive(DataMessage.Record.Type.INSERT, "1200");
        listener.delivered.get(3).ackAsConsumed();
        assertEquals("1200", manager.getSaveCheckpoint());
        listener.delivered.get(2).ackAsConsumed();
        assertEquals("1200", manager.getSaveCheckpoint());
    }

    @Test public void recoveryDropsUndeliveredCacheAndTransactionState() throws Exception {
        UserConfig config = config();
        RecordsCache cache = new RecordsCache();
        cache.setmaxRecordsBatched(1);
        config.setRecordsCache(cache);
        CheckpointManager manager = new CheckpointManager(true);
        CapturingListener listener = new CapturingListener();
        TestConsumer consumer = new TestConsumer(listener);
        consumer.init(config, manager);
        consumer.receive(DataMessage.Record.Type.BEGIN, "1100");
        consumer.receive(DataMessage.Record.Type.INSERT, "1200");
        assertTrue(listener.delivered.isEmpty());
        consumer.init(config, manager);
        consumer.receive(DataMessage.Record.Type.HEARTBEAT, "1300");
        assertEquals(1, listener.delivered.size());
        assertEquals("1300", listener.delivered.get(0).getSafeTimestamp());
    }

    @Test public void singleServiceWithoutAckTrackingFallsBackConservatively() throws Exception {
        CheckpointManager manager = new CheckpointManager(false);
        UserConfig config = config();
        CapturingListener listener = new CapturingListener();
        TestConsumer consumer = new TestConsumer(listener);
        consumer.init(config, manager);
        consumer.receive(DataMessage.Record.Type.INSERT, "1200");
        consumer.receive(DataMessage.Record.Type.HEARTBEAT, "9000");
        consumer.init(config, manager);
        consumer.getPosition();
        assertEquals(1000L, consumer.requestedTimestamp);
    }

    @Test public void oldProducerCannotPutRecordsIntoRecoveredCache() throws Exception {
        UserConfig config = config();
        RecordsCache cache = new RecordsCache();
        cache.setmaxRecordsBatched(1);
        config.setRecordsCache(cache);
        CheckpointManager manager = new CheckpointManager(true);
        CapturingListener listener = new CapturingListener();
        TestConsumer old = new TestConsumer(listener);
        old.init(config, manager);
        TestConsumer recovered = new TestConsumer(listener);
        recovered.init(config, manager);
        old.receive(DataMessage.Record.Type.BEGIN, "9000");
        recovered.receive(DataMessage.Record.Type.HEARTBEAT, "1300");
        assertEquals(1, listener.delivered.size());
        assertEquals("1300", listener.delivered.get(0).getSafeTimestamp());
    }

    @Test public void outOfOrderAndDuplicateAcksCannotSkipPendingRecords() throws Exception {
        CheckpointManager manager = new CheckpointManager(true);
        CapturingListener listener = new CapturingListener();
        TestConsumer consumer = new TestConsumer(listener);
        UserConfig config = config();
        consumer.init(config, manager);
        consumer.receive(DataMessage.Record.Type.INSERT, "1100");
        consumer.receive(DataMessage.Record.Type.INSERT, "1200");
        listener.delivered.get(1).ackAsConsumed();
        listener.delivered.get(1).ackAsConsumed();
        assertNull(manager.getSaveCheckpoint());
        consumer.init(config, manager);
        consumer.getPosition();
        assertEquals(1000L, consumer.requestedTimestamp);
        listener.delivered.get(0).ackAsConsumed();
        assertNull(manager.getSaveCheckpoint());
        consumer.receive(DataMessage.Record.Type.INSERT, "1100");
        listener.delivered.get(2).ackAsConsumed();
        listener.delivered.get(2).ackAsConsumed();
        assertEquals("1100", manager.getSaveCheckpoint());
    }

    @Test(expected = DStoreOffsetNotExistException.class)
    public void missingSafePositionFailsInsteadOfSkippingForward() {
        TestConsumer consumer = new TestConsumer(new CapturingListener());
        consumer.init(config(), new CheckpointManager(true));
        consumer.missing = true;
        consumer.getPosition();
    }
}
