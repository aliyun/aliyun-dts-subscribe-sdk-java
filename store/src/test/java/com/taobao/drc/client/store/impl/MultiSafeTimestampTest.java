package com.taobao.drc.client.store.impl;

import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.message.DataMessage;
import io.netty.util.concurrent.Future;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Verifies getMultiSafeTimestamp semantics and, most importantly, the object-identity
 * invariant fixed for the DRCStore/DStore multi-service path: the CheckpointManager
 * registered in checkpointManagerMap MUST be the very same instance that the consuming
 * side acks records on. Otherwise getMultiSafeTimestamp always reads null.
 */
public class MultiSafeTimestampTest {

    /** Minimal concrete AbstractStoreClient exposing checkpointManagerMap for the test. */
    private static class TestStoreClient extends AbstractStoreClient {
        TestStoreClient() {
            super(null, null, null, null, null, null);
        }
        void register(String subTopic, CheckpointManager cm) {
            checkpointManagerMap.put(subTopic, cm);
        }
        @Override public Future writeUserCtlMessage(byte[] b) { return null; }
        @Override public void stopService() { }
        @Override public Callable<Boolean> startService() { return null; }
        @Override public Callable<Boolean> startMultiService() { return null; }
        @Override public DBType getRunningDBType() { return null; }
    }

    private static DataMessage.Record record(String safeTimestamp) {
        DataMessage.Record r = new DataMessage.Record();
        r.setSafeTimestamp(safeTimestamp);
        return r;
    }

    /** Simulate the consuming side: add the record to the CM's list, then ack it. */
    private static void consumeAndAck(CheckpointManager cm, DataMessage.Record r) {
        cm.addRecord(r);
        cm.removeRecord(r);
    }

    @Test
    public void emptyMapReturnsNull() {
        TestStoreClient client = new TestStoreClient();
        assertNull(client.getMultiSafeTimestamp());
    }

    @Test
    public void allRegisteredButNoneAckedReturnsNull() {
        TestStoreClient client = new TestStoreClient();
        client.register("t-0", new CheckpointManager(true));
        client.register("t-1", new CheckpointManager(true));
        // never consumed/acked -> every saveCheckpoint is null -> break -> null
        assertNull(client.getMultiSafeTimestamp());
    }

    @Test
    public void returnsMinAcrossAllWhenEveryoneAcked() {
        TestStoreClient client = new TestStoreClient();
        CheckpointManager cm0 = new CheckpointManager(true);
        CheckpointManager cm1 = new CheckpointManager(true);
        CheckpointManager cm2 = new CheckpointManager(true);
        consumeAndAck(cm0, record("3000"));
        consumeAndAck(cm1, record("1000"));
        consumeAndAck(cm2, record("2000"));
        client.register("t-0", cm0);
        client.register("t-1", cm1);
        client.register("t-2", cm2);
        assertEquals("1000", client.getMultiSafeTimestamp());
    }

    /**
     * Regression for the fixed bug: when the CheckpointManager registered in the map
     * is the SAME instance the consuming side acks on, getMultiSafeTimestamp returns
     * the advanced value.
     */
    @Test
    public void sameInstanceRegisteredAndAcked_returnsValue() {
        TestStoreClient client = new TestStoreClient();
        CheckpointManager cm = new CheckpointManager(true);
        client.register("t-0", cm);      // registered into map (getMultiSafeTimestamp reads this)
        consumeAndAck(cm, record("1234")); // consuming side acks the SAME instance
        assertEquals("1234", client.getMultiSafeTimestamp());
    }

    /**
     * Reproduces the original defect: the map holds one CheckpointManager while the
     * consuming side acks a DIFFERENT instance. The map instance never advances, so
     * getMultiSafeTimestamp stays null. This is exactly what the heapdump showed and
     * what the fix prevents.
     */
    @Test
    public void differentInstanceAcked_reproducesNull() {
        TestStoreClient client = new TestStoreClient();
        CheckpointManager registered = new CheckpointManager(true);
        CheckpointManager consumed = new CheckpointManager(true); // the self-built one in the bug
        client.register("t-0", registered);
        consumeAndAck(consumed, record("1234")); // ack advances the wrong object
        assertNull(client.getMultiSafeTimestamp());
    }
}
