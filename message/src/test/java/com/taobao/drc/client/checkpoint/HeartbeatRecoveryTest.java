package com.taobao.drc.client.checkpoint;

import com.taobao.drc.client.message.DataMessage.Record;
import org.junit.Test;
import static org.junit.Assert.*;

public class HeartbeatRecoveryTest {
    private static Record record(Record.Type type, String timestamp) {
        Record record = new Record(); record.setType(type);
        record.addAttribute("timestamp", timestamp); record.setSafeTimestamp(timestamp); return record;
    }

    @Test public void heartbeatInsideAckedSuffixWaitsForGapAndSurvivesTrailingDml() {
        CheckpointManager owner = new CheckpointManager(true);
        CheckpointManager recovery = owner.beginRecovery("1000");
        Record first = record(Record.Type.INSERT, "1200");
        Record heartbeat = record(Record.Type.HEARTBEAT, "1100");
        Record last = record(Record.Type.INSERT, "1300");
        recovery.addRecord(first); recovery.addRecord(heartbeat); recovery.addRecord(last);
        heartbeat.ackAsConsumed(); last.ackAsConsumed();
        assertNull(owner.getConfirmedHeartbeat());
        first.ackAsConsumed();
        assertEquals("1100", owner.getConfirmedHeartbeat());
        assertEquals("1300", owner.getSaveCheckpoint());
    }

    @Test public void rightmostDeliveredHeartbeatWinsEvenWhenTimestampMovesBackwards() {
        CheckpointManager owner = new CheckpointManager(true);
        CheckpointManager recovery = owner.beginRecovery("1000");
        Record first = record(Record.Type.HEARTBEAT, "1400");
        Record second = record(Record.Type.HEARTBEAT, "1200");
        Record last = record(Record.Type.INSERT, "1500");
        recovery.addRecord(first); recovery.addRecord(second); recovery.addRecord(last);
        last.ackAsConsumed(); second.ackAsConsumed(); first.ackAsConsumed();
        assertEquals("1200", owner.getConfirmedHeartbeat());
    }

    @Test public void lateHeartbeatAckCannotChangeReplacementGeneration() {
        CheckpointManager owner = new CheckpointManager(true);
        CheckpointManager first = owner.beginRecovery("1000");
        Record old = record(Record.Type.HEARTBEAT, "9000"); first.addRecord(old);
        CheckpointManager second = owner.beginRecovery("1000");
        Record current = record(Record.Type.HEARTBEAT, "1200"); second.addRecord(current);
        current.ackAsConsumed(); old.ackAsConsumed();
        assertEquals("1200", owner.getConfirmedHeartbeat());
    }
}
