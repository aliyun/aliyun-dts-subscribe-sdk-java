package com.taobao.drc.client.network;

import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.impl.RecordsCache;
import com.taobao.drc.client.message.DataMessage;
import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class RecordNotifyHelperTest {
    private static UserConfig config() {
        UserConfig c = new UserConfig(); c.setCheckpoint(new Checkpoint());
        c.getCheckpoint().setTimestamp("1000"); return c;
    }
    private static DataMessage.Record record(DataMessage.Record.Type type, String timestamp) {
        DataMessage.Record r = new DataMessage.Record(); r.setType(type);
        r.addAttribute("timestamp", timestamp); r.addAttribute("source_type", "mysql"); return r;
    }
    private static class AckListener implements Listener {
        final List<DataMessage.Record> delivered = new ArrayList<DataMessage.Record>();
        public void notify(DataMessage m) { for (DataMessage.Record r : m.getRecordList()) { delivered.add(r); r.ackAsConsumed(); } }
        public void notifyRuntimeLog(String level, String message) { }
        public void handleException(Exception e) { throw new AssertionError(e); }
    }
    @Test public void drcSingleHeartbeatAckIsANoOp() throws Exception {
        UserConfig c = config(); AckListener l = new AckListener(); CheckpointManager m = new CheckpointManager(false);
        new RecordNotifyHelper(c, m, l).process(record(DataMessage.Record.Type.HEARTBEAT, "1200"));
        assertEquals(1, l.delivered.size()); assertNull(m.getSaveCheckpoint());
        assertNull(l.delivered.get(0).getCheckpointManager()); assertEquals("1200", c.getCheckpoint().getTimestamp());
    }
    @Test public void rollbackEndsTransactionSafetyState() throws Exception {
        UserConfig c = config(); AckListener l = new AckListener(); CheckpointManager m = new CheckpointManager(false);
        RecordNotifyHelper h = new RecordNotifyHelper(c, m.beginRecovery("1000"), l);
        h.process(record(DataMessage.Record.Type.BEGIN, "1100"));
        h.process(record(DataMessage.Record.Type.ROLLBACK, "1200"));
        h.process(record(DataMessage.Record.Type.HEARTBEAT, "1300"));
        assertEquals("1200", l.delivered.get(1).getSafeTimestamp()); assertEquals("1300", m.getSaveCheckpoint());
    }
    @Test public void retiredHelperDoesNotDeliverOrUpdateSharedHeartbeat() throws Exception {
        UserConfig c = config(); AckListener l = new AckListener(); CheckpointManager m = new CheckpointManager(false);
        RecordNotifyHelper old = new RecordNotifyHelper(c, m.beginRecovery("1000"), l);
        m.beginRecovery("1000"); old.process(record(DataMessage.Record.Type.HEARTBEAT, "9000"));
        assertTrue(l.delivered.isEmpty()); assertEquals("1000", c.getCheckpoint().getTimestamp());
    }
    @Test public void actualTransactionBatchMayExceedTheDeliveryThresholdWithoutFreezing() throws Exception {
        UserConfig c = config(); RecordsCache cache = new RecordsCache(); cache.setmaxRecordsBatched(512); c.setRecordsCache(cache);
        AckListener l = new AckListener(); CheckpointManager m = new CheckpointManager(false);
        RecordNotifyHelper h = new RecordNotifyHelper(c, m.beginRecovery("1000"), l);
        h.process(record(DataMessage.Record.Type.BEGIN, "1100")); h.process(record(DataMessage.Record.Type.INSERT, "1100"));
        h.process(record(DataMessage.Record.Type.COMMIT, "1100")); h.process(record(DataMessage.Record.Type.BEGIN, "1200"));
        for (int i = 0; i < 10237; i++) h.process(record(DataMessage.Record.Type.INSERT, "1200"));
        h.process(record(DataMessage.Record.Type.COMMIT, "1200"));
        assertEquals(10242, l.delivered.size()); assertEquals("1200", m.getSaveCheckpoint());
    }
    @Test public void cacheDoesNotDropDmlWhenTransactionMarkersAreAbsent() throws Exception {
        UserConfig c = config(); c.setRequireTx(false); RecordsCache cache = new RecordsCache();
        cache.setmaxRecordsBatched(1); c.setRecordsCache(cache); AckListener l = new AckListener();
        CheckpointManager m = new CheckpointManager(false);
        new RecordNotifyHelper(c, m.beginRecovery("1000"), l).process(record(DataMessage.Record.Type.INSERT, "1200"));
        assertEquals(1, l.delivered.size()); assertEquals("1200", m.getSaveCheckpoint());
    }

    private static class ListenerAdapter implements Listener {
        public void notify(DataMessage message) throws Exception { }
        public void notifyRuntimeLog(String level, String message) { }
        public void handleException(Exception error) { }
    }

    @Test public void singleServiceConfirmsSuccessfulCallbackWithoutAck() throws Exception {
        CheckpointManager owner = new CheckpointManager(false);
        new RecordNotifyHelper(config(), owner.beginRecovery("1000"), new ListenerAdapter())
                .process(record(DataMessage.Record.Type.INSERT, "1200"));
        assertEquals("1200", owner.prepareRecovery("9000"));
        assertEquals(0L, owner.getPendingCount());
    }

    @Test public void singleAckInsideFailingCallbackCannotConfirmBatch() throws Exception {
        final CheckpointManager owner = new CheckpointManager(false);
        final Exception expected = new Exception("business write failed");
        RecordNotifyHelper helper = new RecordNotifyHelper(config(), owner.beginRecovery("1000"), new ListenerAdapter() {
            @Override public void notify(DataMessage message) throws Exception {
                message.getRecordList().get(0).ackAsConsumed();
                assertNull(owner.getSaveCheckpoint());
                throw expected;
            }
        });
        try { helper.process(record(DataMessage.Record.Type.INSERT, "1200")); fail("notify failure was swallowed"); }
        catch (Exception actual) { assertSame(expected, actual); }
        helper.process(record(DataMessage.Record.Type.HEARTBEAT, "9000"));
        assertEquals("1000", owner.prepareRecovery("9000"));
    }

    @Test public void singleBatchBoundaryIsCapturedBeforeUserMutation() throws Exception {
        final CheckpointManager owner = new CheckpointManager(false);
        RecordNotifyHelper helper = new RecordNotifyHelper(config(), owner.beginRecovery("1000"), new ListenerAdapter() {
            @Override public void notify(DataMessage message) {
                message.getRecordList().get(0).setSafeTimestamp("9999");
                message.getRecordList().clear();
            }
        });
        helper.process(record(DataMessage.Record.Type.INSERT, "1200"));
        assertEquals("1200", owner.getSaveCheckpoint());
    }

    @Test public void cachedHeartbeatCannotAdvanceUntilDeliveredSuccessfully() throws Exception {
        UserConfig c = config(); RecordsCache cache = new RecordsCache();
        cache.setmaxRecordsBatched(2); cache.setmaxTimeoutBatched(Long.MAX_VALUE); c.setRecordsCache(cache);
        new RecordNotifyHelper(c, new CheckpointManager(false), new ListenerAdapter())
                .process(record(DataMessage.Record.Type.HEARTBEAT, "1200"));
        assertEquals("1000", c.getCheckpoint().getTimestamp());
    }

    @Test public void exceptionHandlerFailureDoesNotHideNotifyFailure() throws Exception {
        final Exception original = new Exception("write failed");
        final RuntimeException secondary = new RuntimeException("handler failed");
        RecordNotifyHelper helper = new RecordNotifyHelper(config(), new CheckpointManager(false), new ListenerAdapter() {
            @Override public void notify(DataMessage message) throws Exception { throw original; }
            @Override public void handleException(Exception e) { throw secondary; }
        });
        try { helper.process(record(DataMessage.Record.Type.INSERT, "1200")); fail("expected notify failure"); }
        catch (Exception actual) { assertSame(original, actual); assertArrayEquals(new Throwable[]{secondary}, actual.getSuppressed()); }
    }
}
