package com.taobao.drc.client.checkpoint;

import com.taobao.drc.client.message.DataMessage;
import org.junit.Test;
import java.io.ObjectStreamClass;
import java.util.*;
import static org.junit.Assert.*;

public class AcknowledgementRegressionTest {
    private static DataMessage.Record record(String timestamp) {
        DataMessage.Record r = new DataMessage.Record();
        r.setSafeTimestamp(timestamp);
        return r;
    }

    @Test public void unregisteredAckIsSafe() {
        CheckpointManager m = new CheckpointManager(false);
        DataMessage.Record r = record("1000");
        r.setCheckpointManager(m);
        r.ackAsConsumed();
        assertNull(m.getSaveCheckpoint());
    }

    @Test public void duplicateAckCannotRollbackOrCorruptTheList() {
        CheckpointManager m = new CheckpointManager(true);
        DataMessage.Record a = record("1000"), b = record("2000"), c = record("3000");
        m.addRecord(a); m.addRecord(b); m.addRecord(c);
        m.removeRecord(a); m.removeRecord(b); m.removeRecord(a);
        assertEquals("2000", m.getSaveCheckpoint());
        assertNull(a.getPrev()); assertNull(a.getNext());
        m.removeRecord(c);
        assertEquals("3000", m.getSaveCheckpoint());
    }

    @Test public void foreignAckCannotRemoveTheOwnersRecord() {
        CheckpointManager owner = new CheckpointManager(true), other = new CheckpointManager(true);
        DataMessage.Record r = record("1000");
        owner.addRecord(r);
        other.removeRecord(r);
        assertNull(other.getSaveCheckpoint());
        owner.removeRecord(r);
        assertEquals("1000", owner.getSaveCheckpoint());
    }

    @Test public void duplicateRegistrationIsIdempotent() {
        CheckpointManager m = new CheckpointManager(true);
        DataMessage.Record a = record("1000"), b = record("2000");
        m.addRecord(a); m.addRecord(a); m.addRecord(b);
        m.removeRecord(a);
        assertEquals("1000", m.getSaveCheckpoint());
        m.removeRecord(b);
        assertEquals("2000", m.getSaveCheckpoint());
    }

    @Test public void reorderedAcknowledgementsEventuallyConfirmTheWholePrefix() {
        for (boolean dts : new boolean[] { false, true }) {
            CheckpointManager owner = new CheckpointManager(true);
            CheckpointManager m = dts ? owner.beginRecovery("0") : owner;
            List<DataMessage.Record> records = new ArrayList<DataMessage.Record>();
            for (int i = 0; i < 128; i++) { records.add(record(Integer.toString(i + 1))); m.addRecord(records.get(i)); }
            List<Integer> order = new ArrayList<Integer>();
            for (int i = 0; i < 128; i++) order.add(i);
            Collections.shuffle(order, new Random(23));
            boolean[] acked = new boolean[128];
            int prefix = 0;
            for (int index : order) {
                m.removeRecord(records.get(index));
                m.removeRecord(records.get(index));
                acked[index] = true;
                while (prefix < 128 && acked[prefix]) prefix++;
                assertEquals(prefix == 0 ? null : Integer.toString(prefix), owner.getSaveCheckpoint());
            }
            assertEquals("128", owner.getSaveCheckpoint());
        }
    }

    @Test public void recoveryDoesNotLinkUnacknowledgedPayloads() {
        CheckpointManager.Recovery m = new CheckpointManager(true).beginRecovery("0");
        DataMessage.Record r = record("1000");
        m.addRecord(r);
        assertNull(r.getPrev());
        assertNull(r.getNext());
    }

    @Test public void largeInflightWindowCanStillAdvanceAfterAllAcks() {
        for (boolean multi : new boolean[] { false, true }) {
            CheckpointManager owner = new CheckpointManager(multi);
            CheckpointManager m = owner.beginRecovery("0");
            List<DataMessage.Record> records = new ArrayList<DataMessage.Record>();
            for (int i = 0; i < 32768; i++) { records.add(record(Integer.toString(i + 1))); m.addRecord(records.get(i)); }
            if (!multi) { m.completeDelivery(m.beginDelivery(records)); }
            for (DataMessage.Record r : records) m.removeRecord(r);
            assertEquals("32768", owner.getSaveCheckpoint());
        }
    }

    @Test public void recordSerializationIdentityRemainsCompatible() {
        assertEquals(-4011255769041178611L, ObjectStreamClass.lookup(DataMessage.Record.class).getSerialVersionUID());
    }

    @Test public void adjacentRangesMergeAndClosingGapAtBudgetStillAdvances() {
        CheckpointManager owner = new CheckpointManager(true);
        owner.setMaxPendingAckRanges(1);
        CheckpointManager m = owner.beginRecovery("0");
        List<DataMessage.Record> records = new ArrayList<DataMessage.Record>();
        for (int i = 1; i <= 5; i++) { DataMessage.Record r = record("" + i); records.add(r); m.addRecord(r); }
        records.get(1).ackAsConsumed(); records.get(2).ackAsConsumed();
        assertEquals(1, owner.getPendingAckRangeCount());
        records.get(0).ackAsConsumed();
        assertEquals("3", owner.getSaveCheckpoint());
        assertEquals(0, owner.getPendingAckRangeCount());
        records.get(4).ackAsConsumed(); records.get(3).ackAsConsumed();
        assertEquals("5", owner.getSaveCheckpoint());
        assertEquals(0L, owner.getPendingCount());
        assertFalse(owner.isTrackingOverflowed());
    }

    @Test public void explicitRangeBudgetFailsAttemptAtLastConfirmedBoundary() {
        for (boolean multi : new boolean[] {true}) {
            CheckpointManager owner = new CheckpointManager(multi);
            owner.setMaxPendingAckRanges(1);
            CheckpointManager m = owner.beginRecovery("0");
            List<DataMessage.Record> records = new ArrayList<DataMessage.Record>();
            for (int i = 1; i <= 6; i++) { DataMessage.Record r = record("" + i); records.add(r); m.addRecord(r); }
            records.get(1).ackAsConsumed();
            try { records.get(3).ackAsConsumed(); fail("Overflow must fail the subscription"); }
            catch (IllegalStateException expected) { assertSame(expected, owner.getFailure()); }
            assertTrue(owner.isTrackingOverflowed());
            assertFalse(m.isActive());
            assertEquals(4L, owner.getPendingCount());
            for (DataMessage.Record r : records) { r.ackAsConsumed(); r.ackAsConsumed(); }
            assertEquals(4L, owner.getPendingCount());
            assertNull(owner.getSaveCheckpoint());
            assertEquals("0", owner.prepareRecovery("9000"));
            assertFalse(owner.isTrackingOverflowed());
            CheckpointManager recovered = owner.beginRecovery("9000");
            DataMessage.Record next = record("100"); recovered.addRecord(next); next.ackAsConsumed();
            records.get(5).ackAsConsumed();
            assertEquals("100", owner.getSaveCheckpoint());
        }
    }

    @Test public void ackUsesCapturedBoundaryWithoutMutatingPublicTimestamps() {
        for (boolean dts : new boolean[] {false, true}) {
            CheckpointManager owner = new CheckpointManager(true);
            CheckpointManager m = dts ? owner.beginRecovery("0") : owner;
            DataMessage.Record a = record("1"), b = record("2");
            m.addRecord(a); m.addRecord(b); b.setSafeTimestamp("9000");
            b.ackAsConsumed();
            assertEquals("1", a.getSafeTimestamp());
            a.ackAsConsumed();
            assertEquals("2", owner.getSaveCheckpoint());
        }
    }

    @Test public void deserializedRecordCannotAcknowledgeOriginalDelivery() throws Exception {
        CheckpointManager owner = new CheckpointManager(true);
        CheckpointManager m = owner.beginRecovery("0");
        DataMessage.Record original = record("1000"); m.addRecord(original);
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        new java.io.ObjectOutputStream(bytes).writeObject(original);
        DataMessage.Record copy = (DataMessage.Record) new java.io.ObjectInputStream(
                new java.io.ByteArrayInputStream(bytes.toByteArray())).readObject();
        copy.ackAsConsumed();
        assertNull(copy.getAcknowledgement());
        assertNull(owner.getSaveCheckpoint());
        assertEquals(1L, owner.getPendingCount());
        original.ackAsConsumed();
        assertEquals("1000", owner.getSaveCheckpoint());
    }

    @Test public void statusSnapshotRemainsStableAcrossLaterAcknowledgements() {
        CheckpointManager owner = new CheckpointManager(true);
        assertTrue(owner.getStatus().isAckTrackingEnabled());
        CheckpointManager m = owner.beginRecovery("0");
        DataMessage.Record r = record("1000"); m.addRecord(r);
        CheckpointManager.Status before = owner.getStatus();
        r.ackAsConsumed();
        assertTrue(before.isAckTrackingEnabled());
        assertEquals(1L, before.getPendingCount());
        assertNull(before.getSafeTimestamp());
        assertEquals(0L, owner.getStatus().getPendingCount());
        assertEquals("1000", owner.getStatus().getSafeTimestamp());
    }

    @Test public void concurrentDuplicateAcksConfirmExactlyOnce() throws Exception {
        for (boolean dts : new boolean[] {false, true}) {
            CheckpointManager owner = new CheckpointManager(true);
            CheckpointManager m = dts ? owner.beginRecovery("0") : owner;
            final List<DataMessage.Record> records = new ArrayList<DataMessage.Record>();
            for (int i = 1; i <= 2048; i++) { DataMessage.Record r = record("" + i); records.add(r); m.addRecord(r); }
            java.util.concurrent.ExecutorService workers = java.util.concurrent.Executors.newFixedThreadPool(4);
            try {
                List<java.util.concurrent.Future<?>> done = new ArrayList<java.util.concurrent.Future<?>>();
                for (int worker = 0; worker < 4; worker++) {
                    final int seed = worker;
                    done.add(workers.submit(() -> {
                        List<DataMessage.Record> order = new ArrayList<DataMessage.Record>(records);
                        Collections.shuffle(order, new Random(seed));
                        for (DataMessage.Record r : order) r.ackAsConsumed();
                    }));
                }
                for (java.util.concurrent.Future<?> future : done) future.get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals("2048", owner.getSaveCheckpoint());
                assertEquals(0L, owner.getPendingCount());
            } finally { workers.shutdownNow(); }
        }
    }
}
