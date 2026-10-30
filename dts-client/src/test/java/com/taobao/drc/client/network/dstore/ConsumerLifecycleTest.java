package com.taobao.drc.client.network.dstore;

import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.network.DStoreNetworkEndpoint;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;

public class ConsumerLifecycleTest {
    static class Consumer extends BaseDStoreConsumer {
        final CountDownLatch polling = new CountDownLatch(1);
        final AtomicInteger polls = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        RuntimeException failure;
        Consumer() { this(new CheckpointManager(false)); }
        Consumer(CheckpointManager manager) {
            super(new RecoveryPositionTest.CapturingListener());
            setDaemon(true);
            init(RecoveryPositionTest.config(), manager);
        }
        List<DataMessage.Record> poll(long timeout) throws InterruptedException {
            if (failure != null) throw failure;
            if (polls.incrementAndGet() > 1) throw new IllegalStateException("polled again after interrupt");
            polling.countDown();
            new CountDownLatch(1).await();
            return Collections.emptyList();
        }
        void doClose() { closes.incrementAndGet(); }
        Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) { return null; }
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestamps) { return null; }
    }

    @Test public void interruptionTerminatesAndClosesBeforeSyncReturns() throws Exception {
        Consumer consumer = new Consumer();
        consumer.start();
        assertTrue(consumer.polling.await(2, TimeUnit.SECONDS));
        consumer.interrupt();
        consumer.join(2000);
        assertFalse(consumer.isAlive());
        consumer.sync();
        assertEquals(1, consumer.polls.get());
        assertEquals(1, consumer.closes.get());
    }

    @Test public void workerFailureIsPropagatedAfterCleanup() throws Exception {
        Consumer consumer = new Consumer();
        consumer.failure = new DStoreOffsetNotExistException("missing recovery boundary");
        consumer.start();
        consumer.join(2000);
        assertFalse(consumer.isAlive());
        try {
            consumer.sync();
            fail("Worker failure must reach the reconnect caller");
        } catch (DStoreOffsetNotExistException expected) {
            assertSame(consumer.failure, expected);
        }
        assertEquals(1, consumer.closes.get());
    }

    @Test public void closingAnUnstartedConsumerIsIdempotent() {
        Consumer consumer = new Consumer();
        consumer.close();
        consumer.close();
        assertEquals(1, consumer.closes.get());
    }

    @Test public void endpointCleansUpWhenInitializationFails() {
        final Consumer consumer = new Consumer() {
            boolean initialized;
            @Override public void init(UserConfig config, CheckpointManager manager) {
                super.init(config, manager);
                if (initialized) throw new IllegalStateException("seek failed");
                initialized = true;
            }
        };
        DStoreNetworkEndpoint endpoint = new DStoreNetworkEndpoint() {
            @Override protected DStoreConsumer createConsumer() { return consumer; }
        };
        UserConfig config = RecoveryPositionTest.config(); config.setSubTopic("test-0");
        try {
            endpoint.connectToStoreThenWait(config, new CheckpointManager(false));
            fail("Expected init failure");
        } catch (IllegalStateException expected) {
            assertEquals("seek failed", expected.getMessage());
        }
        assertEquals(1, consumer.closes.get());
        assertEquals(0, consumer.polls.get());
    }

    @Test public void endpointClosedDuringInitializationCannotStartLateWorker() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        final Consumer consumer = new Consumer() {
            boolean initialized;
            @Override public void init(UserConfig config, CheckpointManager manager) {
                super.init(config, manager);
                if (initialized) {
                    entered.countDown();
                    try { release.await(); }
                    catch (InterruptedException e) { throw new AssertionError(e); }
                }
                initialized = true;
            }
        };
        final DStoreNetworkEndpoint endpoint = new DStoreNetworkEndpoint() {
            @Override protected DStoreConsumer createConsumer() { return consumer; }
        };
        final UserConfig config = RecoveryPositionTest.config(); config.setSubTopic("test-0");
        java.util.concurrent.ExecutorService caller = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            java.util.concurrent.Future<?> call = caller.submit(() -> endpoint.connectToStoreThenWait(config, new CheckpointManager(false)));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            endpoint.close();
            release.countDown();
            call.get(2, TimeUnit.SECONDS);
            assertEquals(0, consumer.polls.get());
            assertEquals(1, consumer.closes.get());
        } finally {
            release.countDown();
            endpoint.close();
            caller.shutdownNow();
        }
    }

    @Test public void stopDuringRegistrationFencesSubsequentConnection() {
        DStoreNetworkEndpoint endpoint = new DStoreNetworkEndpoint() {
            @Override protected DStoreConsumer createConsumer() { throw new AssertionError("Started after stop"); }
        };
        long registrationGeneration = endpoint.getGeneration();
        endpoint.close();
        UserConfig config = RecoveryPositionTest.config(); config.setSubTopic("test-0");
        endpoint.connectToStoreThenWait(config, new CheckpointManager(false), registrationGeneration);
    }

    @Test public void asyncAckOverflowStopsEvenAnIdleConsumer() throws Exception {
        CheckpointManager owner = new CheckpointManager(true);
        owner.setMaxPendingAckRanges(1);
        Consumer consumer = new Consumer(owner);
        DataMessage.Record[] records = new DataMessage.Record[4];
        for (int i = 0; i < records.length; i++) {
            records[i] = new DataMessage.Record(); records[i].setType(DataMessage.Record.Type.INSERT);
            records[i].addAttribute("source_type", "mysql"); records[i].addAttribute("timestamp", "1100");
            consumer.notifyHeper.process(records[i]);
        }
        consumer.start();
        assertTrue(consumer.polling.await(2, TimeUnit.SECONDS));
        try {
            records[1].ackAsConsumed();
            try { records[3].ackAsConsumed(); fail("Expected ACK budget failure"); }
            catch (IllegalStateException expected) { }
            consumer.join(2000);
            assertFalse("Overflow must wake the idle consumer", consumer.isAlive());
            try { consumer.sync(); fail("sync must propagate the failure"); }
            catch (IllegalStateException expected) { assertSame(owner.getFailure(), expected); }
            assertEquals(1, consumer.closes.get());
        } finally { consumer.close(); consumer.join(2000); }
    }

    @Test public void lateFailureSignalFromOldAttemptCannotCloseReplacement() throws Exception {
        Consumer consumer = new Consumer();
        java.lang.reflect.Field field = BaseDStoreConsumer.class.getDeclaredField("recovery"); field.setAccessible(true);
        CheckpointManager previous = (CheckpointManager) field.get(consumer);
        consumer.prepareRecovery();
        try {
            previous.fail(new IllegalStateException("old failure"));
            assertEquals(0, consumer.closes.get());
            assertTrue(((CheckpointManager) field.get(consumer)).isActive());
        } finally { consumer.close(); }
    }
}
