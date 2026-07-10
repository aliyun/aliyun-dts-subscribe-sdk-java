package com.taobao.drc.client.checkpoint;

import com.taobao.drc.client.message.DataMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Verifies the linked-list based checkpoint advancing semantics that
 * getMultiSafeTimestamp relies on: saveCheckpoint only advances when the acked
 * record is currently at the head (the oldest un-acked record).
 */
public class CheckpointManagerTest {

    private static DataMessage.Record record(String safeTimestamp) {
        DataMessage.Record record = new DataMessage.Record();
        record.setSafeTimestamp(safeTimestamp);
        return record;
    }

    @Test
    public void saveCheckpointIsNullBeforeAnyAck() {
        CheckpointManager cm = new CheckpointManager(true);
        assertNull(cm.getSaveCheckpoint());
    }

    @Test
    public void ackingHeadAdvancesSaveCheckpoint() {
        CheckpointManager cm = new CheckpointManager(true);
        DataMessage.Record r1 = record("1000");
        cm.addRecord(r1);

        cm.removeRecord(r1);

        assertEquals("1000", cm.getSaveCheckpoint());
    }

    @Test
    public void ackingNonHeadDoesNotAdvanceUntilHeadAcked() {
        CheckpointManager cm = new CheckpointManager(true);
        DataMessage.Record r1 = record("1000");
        DataMessage.Record r2 = record("2000");
        cm.addRecord(r1);
        cm.addRecord(r2);

        // ack the second record first: it is not at the head, so saveCheckpoint must NOT advance
        cm.removeRecord(r2);
        assertNull(cm.getSaveCheckpoint());

        // now ack the head record: saveCheckpoint advances to the head's safeTimestamp
        cm.removeRecord(r1);
        assertEquals("1000", cm.getSaveCheckpoint());
    }

    @Test
    public void ackingInOrderAdvancesToLatest() {
        CheckpointManager cm = new CheckpointManager(true);
        DataMessage.Record r1 = record("1000");
        DataMessage.Record r2 = record("2000");
        cm.addRecord(r1);
        cm.addRecord(r2);

        cm.removeRecord(r1);
        assertEquals("1000", cm.getSaveCheckpoint());

        cm.removeRecord(r2);
        assertEquals("2000", cm.getSaveCheckpoint());
    }
}
