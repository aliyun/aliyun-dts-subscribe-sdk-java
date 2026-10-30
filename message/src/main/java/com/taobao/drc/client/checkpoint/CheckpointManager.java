package com.taobao.drc.client.checkpoint;

import com.taobao.drc.client.message.DataMessage.Record;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointManager {
    private static final Logger log = LoggerFactory.getLogger(CheckpointManager.class);
    private volatile String saveCheckpoint;
    private final Record header = new Record();
    private final ReentrantLock lock = new ReentrantLock();
    // Legacy direct add/remove API only. Live DRC/DTS subscriptions use Recovery receipts.
    private final Map<Record, String> pending = new IdentityHashMap<Record, String>();
    private boolean multiMode;
    private String initialRecoveryTimestamp;
    private volatile Recovery activeRecovery;
    private Recovery preparedRecovery;
    private int maxPendingAckRanges;
    private volatile String confirmedHeartbeat;
    private volatile Throwable failure;
    private volatile boolean invalidated;
    private Runnable failureListener;
    private boolean failureNotified;
    private String recoveryReason;
    private long generation;
    private long connectionCount;

    public void setRecoveryReason(String reason) { recoveryReason = reason; }
    public void markConnected() { lock.lock(); try { connectionCount++; } finally { lock.unlock(); } }
    public long getConnectionCount() { lock.lock(); try { return connectionCount; } finally { lock.unlock(); } }

    /** Immutable candidate captured before application code can mutate its message. */
    public static final class Delivery {
        private final CheckpointManager manager;
        private final int count;
        private final String timestamp;
        private final String heartbeat;
        private boolean completed;
        private Delivery(CheckpointManager manager, int count, String timestamp, String heartbeat) {
            this.manager = manager; this.count = count; this.timestamp = timestamp; this.heartbeat = heartbeat;
        }
    }

    protected ReentrantLock stateLock() { return lock; }

    public Delivery beginDelivery(List<Record> records) {
        ReentrantLock state = stateLock(); state.lock();
        try {
            if (!isActive()) { return null; }
            String timestamp = null, heartbeat = null;
            for (Record record : records) {
                timestamp = record.getSafeTimestamp();
                if (record.getOpt() == Record.Type.HEARTBEAT) { heartbeat = record.getTimestamp(); }
                if (isMultiMode()) { addRecord(record); }
            }
            return new Delivery(this, records.size(), timestamp, heartbeat);
        } finally { state.unlock(); }
    }

    public void completeDelivery(Delivery delivery) {
        ReentrantLock state = stateLock(); state.lock();
        try {
            if (delivery != null && delivery.manager == this && isActive() && !isMultiMode()) {
                if (delivery.heartbeat != null) { confirmedHeartbeat = delivery.heartbeat; }
            }
        } finally { state.unlock(); }
    }

    public String getConfirmedHeartbeat() { return confirmedHeartbeat; }

    /** Small SDK state writes share the generation fence with confirmation. */
    public void runIfActive(Runnable action) {
        ReentrantLock state = stateLock(); state.lock();
        try { if (isActive()) { action.run(); } } finally { state.unlock(); }
    }

    public void fail(Throwable cause) {
        ReentrantLock state = stateLock(); state.lock();
        try { if (failure == null) { failure = cause; } } finally { state.unlock(); }
        signalFailure();
    }

    /** The listener must request cancellation without waiting for the consumer thread. */
    public void setFailureListener(Runnable listener) {
        ReentrantLock state = stateLock(); state.lock();
        try { failureListener = listener; } finally { state.unlock(); }
        signalFailure();
    }

    private void signalFailure() {
        Runnable listener = null;
        ReentrantLock state = stateLock(); state.lock();
        try {
            if (failure != null && failureListener != null && !failureNotified) {
                failureNotified = true; listener = failureListener;
            }
        } finally { state.unlock(); }
        if (listener != null) {
            try { listener.run(); }
            catch (Throwable cleanupFailure) { log.error("Cannot cancel failed subscription", cleanupFailure); }
        }
    }

    public Throwable getFailure() { return activeRecovery == null ? failure : activeRecovery.getFailure(); }

    public void invalidate() {
        ReentrantLock state = stateLock(); state.lock();
        try {
            invalidated = true;
            if (activeRecovery != null) { activeRecovery.invalidate(); }
        } finally { state.unlock(); }
    }

    /** Immutable monitoring snapshot for one subscription/shard. */
    public static final class Status {
        private final boolean ackTrackingEnabled;
        private final long pendingCount;
        private final int pendingAckRangeCount;
        private final boolean trackingOverflowed;
        private final String safeTimestamp;
        private final int maxPendingAckRanges;
        private final String recoveryTimestamp;
        private final String confirmedHeartbeat;
        private final String receivedTimestamp;
        private final String failureMessage;
        private final long lastProgressAgeMillis;
        private final boolean active;
        private final long generation;
        private final String channel;
        private final String recoveryReason;
        private final long recoveryProgressAgeMillis;

        private Status(boolean enabled, long pending, int ranges, boolean overflowed, String timestamp, int limit,
                String recovery, String heartbeat, String received, Throwable failure, long age, boolean active,
                long generation, String channel, String reason, long recoveryAge) {
            ackTrackingEnabled = enabled;
            pendingCount = pending;
            pendingAckRangeCount = ranges;
            trackingOverflowed = overflowed;
            safeTimestamp = timestamp;
            maxPendingAckRanges = limit;
            recoveryTimestamp = recovery; confirmedHeartbeat = heartbeat; receivedTimestamp = received;
            failureMessage = failure == null ? null : failure.toString();
            lastProgressAgeMillis = age; this.active = active;
            this.generation = generation; this.channel = channel; recoveryReason = reason;
            recoveryProgressAgeMillis = recoveryAge;
        }
        public boolean isAckTrackingEnabled() { return ackTrackingEnabled; }
        public long getPendingCount() { return pendingCount; }
        public int getPendingAckRangeCount() { return pendingAckRangeCount; }
        public boolean isTrackingOverflowed() { return trackingOverflowed; }
        public String getSafeTimestamp() { return safeTimestamp; }
        public int getMaxPendingAckRanges() { return maxPendingAckRanges; }
        public String getRecoveryTimestamp() { return recoveryTimestamp; }
        public String getConfirmedHeartbeat() { return confirmedHeartbeat; }
        public String getReceivedTimestamp() { return receivedTimestamp; }
        public String getFailureMessage() { return failureMessage; }
        public long getLastProgressAgeMillis() { return lastProgressAgeMillis; }
        public boolean isActive() { return active; }
        public long getGeneration() { return generation; }
        public String getChannel() { return channel; }
        public String getRecoveryReason() { return recoveryReason; }
        public long getRecoveryProgressAgeMillis() { return recoveryProgressAgeMillis; }
    }

    public Status getStatus() {
        lock.lock();
        try {
            return activeRecovery == null ? new Status(multiMode, pending.size(), 0, false, saveCheckpoint, maxPendingAckRanges,
                    initialRecoveryTimestamp, confirmedHeartbeat, null, failure, 0, false, generation, null, recoveryReason, 0)
                    : activeRecovery.getStatus();
        } finally { lock.unlock(); }
    }

    /** SDK-owned, transient receipt. The manager never retains the Record through it. */
    public static final class Acknowledgement {
        private final CheckpointManager manager;
        private final long sequence;
        private final String timestamp;
        private final String heartbeat;
        private boolean acknowledged;

        private Acknowledgement(CheckpointManager manager, long sequence, String timestamp) {
            this(manager, sequence, timestamp, null);
        }
        private Acknowledgement(CheckpointManager manager, long sequence, String timestamp, String heartbeat) {
            this.manager = manager;
            this.sequence = sequence;
            this.timestamp = timestamp;
            this.heartbeat = heartbeat;
        }
    }

    public CheckpointManager(boolean multiMode) {
        this.multiMode = multiMode;
        header.setPrev(header);
        header.setNext(header);
    }

    public void addRecord(Record record) {
        lock.lock();
        try {
            if (alreadyRegistered(record)) { return; }
            record.setAcknowledgement(new Acknowledgement(this, 0, record.getSafeTimestamp()));
            record.setCheckpointManager(this);
            pending.put(record, record.getSafeTimestamp());
            header.getPrev().setNext(record);
            record.setPrev(header.getPrev());
            record.setNext(header);
            header.setPrev(record);
        } finally { lock.unlock(); }
    }

    protected final boolean alreadyRegistered(Record record) {
        Acknowledgement receipt = record.getAcknowledgement();
        if (receipt != null && receipt.manager != this) {
            throw new IllegalArgumentException("Record already belongs to another checkpoint manager");
        }
        return receipt != null;
    }

    public void removeRecord(Record record) {
        lock.lock();
        try {
            Acknowledgement receipt = record.getAcknowledgement();
            if (receipt == null || receipt.manager != this || receipt.acknowledged
                    || !pending.containsKey(record)) { return; }
            receipt.acknowledged = true;
            String boundary = pending.remove(record);
            Record previous = record.getPrev();
            if (previous == header) {
                saveCheckpoint = boundary;
            } else {
                // Remember ACKed successors until the preceding gap is closed.
                pending.put(previous, boundary);
            }
            previous.setNext(record.getNext());
            record.getNext().setPrev(previous);
            record.setPrev(null);
            record.setNext(null);
        } finally { lock.unlock(); }
    }

    public String getSaveCheckpoint() { return saveCheckpoint; }
    public void setSaveCheckpoint(String saveCheckpoint) { this.saveCheckpoint = saveCheckpoint; }
    public boolean isMultiMode() { return multiMode; }
    public void setMultiMode(boolean multiMode) { this.multiMode = multiMode; }
    public boolean isActive() { return !invalidated && failure == null; }

    public void recordReceived(Record record) { }

    public void checkForStalledProgress() {
        Recovery recovery = activeRecovery;
        if (recovery != null) { recovery.checkForStalledProgress(); }
    }

    /** Zero preserves compatibility: no implicit budget on out-of-order ACK ranges. */
    public void setMaxPendingAckRanges(int maximum) {
        if (maximum < 0) {
            throw new IllegalArgumentException("client.maxPendingAckRanges must be >= 0");
        }
        lock.lock();
        try { maxPendingAckRanges = maximum; } finally { lock.unlock(); }
    }

    public int getMaxPendingAckRanges() {
        lock.lock();
        try { return maxPendingAckRanges; } finally { lock.unlock(); }
    }

    /** Delivered minus uniquely acknowledged records in the active attempt. */
    public long getPendingCount() {
        lock.lock();
        try { return activeRecovery == null ? pending.size() : activeRecovery.getPendingCount(); }
        finally { lock.unlock(); }
    }

    /** Stored ACK intervals. Overflow terminates the attempt and clears these intervals. */
    public int getPendingAckRangeCount() {
        lock.lock();
        try { return activeRecovery == null ? 0 : activeRecovery.getPendingAckRangeCount(); }
        finally { lock.unlock(); }
    }

    public boolean isTrackingOverflowed() {
        lock.lock();
        try { return activeRecovery != null && activeRecovery.isTrackingOverflowed(); }
        finally { lock.unlock(); }
    }

    /** Snapshot a DTS restart position and fence the previous attempt's ACKs. */
    public Recovery beginRecovery(String initialTimestamp) {
        return beginRecovery(initialTimestamp, false);
    }

    public Recovery beginDrcRecovery(String initialTimestamp) {
        return beginRecovery(initialTimestamp, true);
    }

    private Recovery beginRecovery(String initialTimestamp, boolean drc) {
        lock.lock();
        try {
            activeRecovery = preparedRecovery != null ? preparedRecovery
                    : new Recovery(this, recoveryTimestamp(initialTimestamp, drc), drc);
            preparedRecovery = null;
            return activeRecovery;
        } finally { lock.unlock(); }
    }

    /** Freeze the registration boundary and fence the old producer before seek. */
    public String prepareRecovery(String initialTimestamp) {
        return prepareRecovery(initialTimestamp, false);
    }

    public String prepareDrcRecovery(String initialTimestamp) {
        return prepareRecovery(initialTimestamp, true);
    }

    private String prepareRecovery(String initialTimestamp, boolean drc) {
        lock.lock();
        try {
            preparedRecovery = new Recovery(this, recoveryTimestamp(initialTimestamp, drc), drc);
            activeRecovery = preparedRecovery;
            return preparedRecovery.getTimestamp();
        } finally { lock.unlock(); }
    }

    /** Select the same safe lower bound for channel registration and recovery. */
    public String getRecoveryTimestamp(String initialTimestamp) {
        return recoveryTimestamp(initialTimestamp, false);
    }

    public String getDrcRecoveryTimestamp(String initialTimestamp) {
        return recoveryTimestamp(initialTimestamp, true);
    }

    private String recoveryTimestamp(String initialTimestamp, boolean drc) {
        lock.lock();
        try {
            if (initialRecoveryTimestamp == null) { initialRecoveryTimestamp = initialTimestamp; }
            String confirmed = drc ? confirmedHeartbeat : saveCheckpoint;
            return confirmed == null ? initialRecoveryTimestamp : confirmed;
        } finally { lock.unlock(); }
    }

    public static final class Recovery extends CheckpointManager {
        private final CheckpointManager owner;
        private final String timestamp;
        private final int rangeLimit;
        private final boolean drc;
        private long delivered;
        private long acknowledged;
        private long confirmedSequence;
        private boolean trackingOverflowed;
        private String receivedTimestamp;
        private long lastProgressNanos = System.nanoTime();
        private long lastWarningNanos = lastProgressNanos;
        private long lastRecoveryProgressNanos = lastProgressNanos;
        private final long generation;
        // Disjoint, nonadjacent ACKed intervals beyond the first unACKed record.
        private final TreeMap<Long, AckRange> ranges = new TreeMap<Long, AckRange>();

        private static final class AckRange {
            private final long end;
            private final String timestamp;
            private final String heartbeat;
            private AckRange(long end, String timestamp, String heartbeat) {
                this.end = end; this.timestamp = timestamp; this.heartbeat = heartbeat;
            }
        }

        private Recovery(CheckpointManager owner, String timestamp, boolean drc) {
            super(owner.isMultiMode());
            this.owner = owner;
            this.timestamp = timestamp;
            this.rangeLimit = owner.maxPendingAckRanges;
            this.drc = drc;
            this.generation = ++owner.generation;
        }

        public String getTimestamp() { return timestamp; }
        @Override public boolean isActive() { return owner.activeRecovery == this && !trackingOverflowed && super.isActive(); }
        @Override protected ReentrantLock stateLock() { return owner.lock; }
        @Override public int getMaxPendingAckRanges() { return rangeLimit; }
        @Override public void markConnected() { owner.markConnected(); }
        @Override public void recordReceived(Record record) {
            owner.lock.lock();
            try { if (isActive()) { receivedTimestamp = record.getTimestamp(); } }
            finally { owner.lock.unlock(); }
        }

        @Override public Delivery beginDelivery(List<Record> records) {
            owner.lock.lock();
            try {
                Delivery delivery = super.beginDelivery(records);
                if (delivery != null && !isMultiMode()) { delivered += delivery.count; }
                return delivery;
            } finally { owner.lock.unlock(); }
        }

        @Override public void completeDelivery(Delivery delivery) {
            owner.lock.lock();
            try {
                if (delivery == null || delivery.manager != this || delivery.completed || !isActive() || isMultiMode()) { return; }
                delivery.completed = true;
                acknowledged += delivery.count;
                lastProgressNanos = System.nanoTime();
                boolean recoveryAdvanced = drc ? delivery.heartbeat != null && !delivery.heartbeat.equals(owner.confirmedHeartbeat)
                        : !java.util.Objects.equals(delivery.timestamp, owner.saveCheckpoint);
                owner.saveCheckpoint = delivery.timestamp;
                setSaveCheckpoint(delivery.timestamp);
                if (recoveryAdvanced) {
                    lastRecoveryProgressNanos = System.nanoTime();
                }
                if (delivery.heartbeat != null) { owner.confirmedHeartbeat = delivery.heartbeat; }
            } finally { owner.lock.unlock(); }
        }

        @Override public String getConfirmedHeartbeat() { return owner.confirmedHeartbeat; }

        @Override public void addRecord(Record record) {
            owner.lock.lock();
            try {
                if (!isMultiMode() || !isActive() || alreadyRegistered(record)) { return; }
                if (delivered == acknowledged) { lastProgressNanos = System.nanoTime(); }
                record.setAcknowledgement(new Acknowledgement(this, ++delivered, record.getSafeTimestamp(),
                        record.getOpt() == Record.Type.HEARTBEAT ? record.getTimestamp() : null));
                record.setCheckpointManager(this);
            } finally { owner.lock.unlock(); }
        }

        @Override public void removeRecord(Record record) {
            CheckpointTrackingException overflow = null;
            owner.lock.lock();
            try {
                Acknowledgement receipt = record.getAcknowledgement();
                if (!isActive() || receipt == null || receipt.manager != this || receipt.acknowledged) {
                    return;
                }
                receipt.acknowledged = true;
                acknowledged++;
                if (trackingOverflowed) { return; }
                long start = receipt.sequence;
                long end = start;
                String boundary = receipt.timestamp;
                String heartbeat = receipt.heartbeat;
                Map.Entry<Long, AckRange> left = ranges.lowerEntry(start);
                if (left != null && left.getValue().end == start - 1) {
                    start = left.getKey();
                    if (heartbeat == null) { heartbeat = left.getValue().heartbeat; }
                    ranges.remove(left.getKey());
                }
                AckRange right = ranges.remove(end + 1);
                if (right != null) {
                    end = right.end;
                    boundary = right.timestamp;
                    if (right.heartbeat != null) { heartbeat = right.heartbeat; }
                }
                if (start == confirmedSequence + 1) {
                    confirmedSequence = end;
                    lastProgressNanos = System.nanoTime();
                    boolean recoveryAdvanced = drc ? heartbeat != null && !heartbeat.equals(owner.confirmedHeartbeat)
                            : !java.util.Objects.equals(boundary, owner.saveCheckpoint);
                    setSaveCheckpoint(boundary);
                    owner.saveCheckpoint = boundary;
                    if (recoveryAdvanced) {
                        lastRecoveryProgressNanos = System.nanoTime();
                    }
                    if (heartbeat != null) { owner.confirmedHeartbeat = heartbeat; }
                } else {
                    ranges.put(start, new AckRange(end, boundary, heartbeat));
                    if (rangeLimit > 0 && ranges.size() > rangeLimit) {
                        trackingOverflowed = true;
                        ranges.clear();
                        overflow = new CheckpointTrackingException("ACK ranges exceeded client.maxPendingAckRanges="
                                + rangeLimit + ". Subscription stopped at its last confirmed recovery boundary. "
                                + "Reduce ACK reordering or increase/disable this optional budget before restarting; "
                                + "unconfirmed records will be replayed. Inspect getCheckpointStatus().");
                    }
                }
            } finally { owner.lock.unlock(); }
            if (overflow != null) {
                log.error(overflow.getMessage());
                fail(overflow);
                throw overflow;
            }
        }

        @Override public void checkForStalledProgress() {
            owner.lock.lock();
            try {
                long now = System.nanoTime();
                if (isActive() && isMultiMode() && delivered > acknowledged
                        && now - lastProgressNanos >= 60000000000L && now - lastWarningNanos >= 60000000000L) {
                    lastWarningNanos = now;
                    log.warn("No contiguous ACK progress for {} ms: pending={}, ACK ranges={}, recovery={}. "
                                    + "ACK every delivered record, including heartbeat and transaction markers; inspect getCheckpointStatus().",
                            new Object[] { (now - lastProgressNanos) / 1000000, delivered - acknowledged, ranges.size(),
                            owner.recoveryTimestamp(timestamp, drc) });
                }
            } finally { owner.lock.unlock(); }
        }

        @Override public long getPendingCount() {
            owner.lock.lock();
            try { return delivered - acknowledged; } finally { owner.lock.unlock(); }
        }

        @Override public int getPendingAckRangeCount() {
            owner.lock.lock();
            try { return ranges.size(); } finally { owner.lock.unlock(); }
        }

        @Override public boolean isTrackingOverflowed() {
            owner.lock.lock();
            try { return trackingOverflowed; } finally { owner.lock.unlock(); }
        }

        @Override public Status getStatus() {
            owner.lock.lock();
            try {
                return new Status(isMultiMode(), delivered - acknowledged, ranges.size(), trackingOverflowed,
                        owner.saveCheckpoint, rangeLimit, owner.recoveryTimestamp(timestamp, drc),
                        owner.confirmedHeartbeat, receivedTimestamp, getFailure(),
                        (System.nanoTime() - lastProgressNanos) / 1000000, isActive(), generation,
                        drc ? "DRC" : "DTS", owner.recoveryReason, (System.nanoTime() - lastRecoveryProgressNanos) / 1000000);
            } finally { owner.lock.unlock(); }
        }
    }
}
