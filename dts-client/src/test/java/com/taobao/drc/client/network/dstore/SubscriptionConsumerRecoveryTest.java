package com.taobao.drc.client.network.dstore;

import com.aliyun.dts.subscribe.clients.recordfetcher.ClusterSwitchListener;
import com.taobao.drc.client.DataFilterBase;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.togo.client.consumer.TogoConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SubscriptionConsumerRecoveryTest {
    // Keep real init/reset/assign/seek code, replacing only remote offset lookup
    // and the poll that supplies the explicit failure. No broker is contacted.
    static class TestConsumer extends SubscriptionConsumer {
        final List<Long> requested = new ArrayList<Long>();
        boolean missing;
        TestConsumer(Listener listener) { super(listener); }
        @Override protected Map<String, Object> consumerConfig(UserConfig config) {
            Map<String, Object> properties = super.consumerConfig(config);
            properties.put("bootstrap.servers", "127.0.0.1:1");
            properties.put("security.protocol", "PLAINTEXT");
            return properties;
        }
        @Override Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestamps) {
            long timestamp = timestamps.get(assignedPartition);
            requested.add(timestamp);
            return Collections.singletonMap(assignedPartition,
                    missing ? null : new OffsetAndTimestamp(timestamp, timestamp));
        }
        @Override Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
            return Collections.singletonMap(assignedPartition, 0L);
        }
        void receive(final String timestamp) throws Exception {
            notifyHeper.process(new DataMessage.Record() {
                @Override public Type getOpt() { return Type.INSERT; }
                @Override public String getTimestamp() { return timestamp; }
                @Override public DBType getDbType() { return DBType.MYSQL; }
            });
        }
    }

    @Test public void offsetOutOfRangeResetsToConfirmedSafeTime() throws Exception {
        recover(new OffsetOutOfRangeException(Collections.singletonMap(new TopicPartition("test-0", 0), 999L)), false);
    }

    @Test public void clusterChangeResetsToConfirmedSafeTime() throws Exception {
        recover(new ClusterSwitchListener.ClusterSwitchException("changed cluster"), false);
    }

    @Test(expected = DStoreOffsetNotExistException.class)
    public void unavailableConfirmedTimeFailsRecovery() throws Exception {
        recover(new ClusterSwitchListener.ClusterSwitchException("changed cluster"), true);
    }

    private void recover(RuntimeException failure, boolean missing) throws Exception {
        RecoveryPositionTest.CapturingListener listener = new RecoveryPositionTest.CapturingListener();
        TestConsumer consumer = new TestConsumer(listener);
        UserConfig config = RecoveryPositionTest.config();
        config.setSubTopic("test-0");
        config.setDataFilter(mock(DataFilterBase.class));
        CheckpointManager manager = new CheckpointManager(true);
        Field field = SubscriptionConsumer.class.getDeclaredField("consumer");
        field.setAccessible(true);
        try {
            consumer.init(config, manager);
            consumer.receive("1100");
            listener.delivered.get(0).ackAsConsumed();
            consumer.receive("1900"); // deliberately left unacknowledged
            ((TogoConsumer) field.get(consumer)).close();
            TogoConsumer broken = mock(TogoConsumer.class);
            when(broken.poll(anyLong())).thenThrow(failure);
            field.set(consumer, broken);
            consumer.missing = missing;
            assertTrue(consumer.poll(1).isEmpty());
            assertEquals(Arrays.asList(1000L, 1100L), consumer.requested);
            verify(broken).close();
            TogoConsumer replacement = (TogoConsumer) field.get(consumer);
            assertEquals(1100L, replacement.position(new TopicPartition("test-0", 0)));
            listener.delivered.get(1).ackAsConsumed();
            assertEquals("1100", manager.getSaveCheckpoint());
            consumer.receive("1200");
            listener.delivered.get(2).ackAsConsumed();
            assertEquals("1200", manager.getSaveCheckpoint());
        } finally {
            consumer.doClose();
        }
    }
}
