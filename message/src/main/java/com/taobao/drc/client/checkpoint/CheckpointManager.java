package com.taobao.drc.client.checkpoint;

import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.message.DataMessage.Record;

import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointManager {
    private static final Logger log = LoggerFactory.getLogger(CheckpointManager.class);
    private static final int SINGLE_SERVICE_MAX_PENDING_ACKS = 10240;

    private volatile String saveCheckpoint;

    private Record header;

    private ReentrantLock lock;

    private boolean multiMode;

    private String initialRecoveryTimestamp;
    private Recovery activeRecovery;
    private Recovery preparedRecovery;

    public CheckpointManager(boolean multiMode) {
        this.multiMode  = multiMode;
        header=new DataMessage.Record();
        header.setPrev(header);
        header.setNext(header);
        lock=new ReentrantLock();
    }


    public void addRecord(DataMessage.Record record){
        try{
            lock.lock();
            header.getPrev().setNext(record);
            record.setPrev(header.getPrev());
            record.setNext(header);
            header.setPrev(record);
        }finally {
            lock.unlock();
        }
    }

    public void removeRecord(DataMessage.Record record){
        try{
            lock.lock();
            if(record.getPrev().equals(header)){
                saveCheckpoint=record.getSafeTimestamp();
            }
            record.getPrev().setNext(record.getNext());
            record.getNext().setPrev(record.getPrev());
        }finally {
            lock.unlock();
        }
    }

    public String getSaveCheckpoint() {
        return saveCheckpoint;
    }

    public void setSaveCheckpoint(String saveCheckpoint) {
        this.saveCheckpoint = saveCheckpoint;
    }

    public boolean isMultiMode() {
        return multiMode;
    }

    public void setMultiMode(boolean multiMode) {
        this.multiMode = multiMode;
    }

    /**
     * Snapshot a DTS restart position and fence acknowledgements from the previous
     * consumption attempt. The shard manager remains registered with the store
     * client; each attempt owns a separate pending-record list.
     */
    public Recovery beginRecovery(String initialTimestamp) {
        lock.lock();
        try {
            activeRecovery = preparedRecovery != null ? preparedRecovery
                    : new Recovery(this, getRecoveryTimestamp(initialTimestamp));
            preparedRecovery = null;
            return activeRecovery;
        } finally {
            lock.unlock();
        }
    }

    /** Freeze the registration boundary and fence the old producer before seek. */
    public String prepareRecovery(String initialTimestamp) {
        lock.lock();
        try {
            preparedRecovery = new Recovery(this, getRecoveryTimestamp(initialTimestamp));
            activeRecovery = preparedRecovery;
            return preparedRecovery.getTimestamp();
        } finally {
            lock.unlock();
        }
    }

    private void clearPendingRecords() {
        lock.lock();
        try {
            Record record = header.getNext();
            while (record != header) {
                Record next = record.getNext();
                record.setPrev(null);
                record.setNext(null);
                record = next;
            }
            header.setNext(header);
            header.setPrev(header);
        } finally {
            lock.unlock();
        }
    }

    /** Select the same safe lower bound for channel registration and recovery. */
    public String getRecoveryTimestamp(String initialTimestamp) {
        lock.lock();
        try {
            if (initialRecoveryTimestamp == null) {
                initialRecoveryTimestamp = initialTimestamp;
            }
            return saveCheckpoint == null ? initialRecoveryTimestamp : saveCheckpoint;
        } finally {
            lock.unlock();
        }
    }

    public static final class Recovery extends CheckpointManager {
        private final CheckpointManager owner;
        private final String timestamp;
        private int pendingCount;
        private boolean trackingOverflowed;

        private Recovery(CheckpointManager owner, String timestamp) {
            // Track delivered records in both DTS modes. This internal flag does
            // not change the public subscription mode or its topic selection.
            super(true);
            this.owner = owner;
            this.timestamp = timestamp;
        }

        public String getTimestamp() {
            return timestamp;
        }

        @Override
        public void addRecord(Record record) {
            owner.lock.lock();
            try {
                if (owner.activeRecovery == this && !trackingOverflowed) {
                    if (!owner.isMultiMode() && pendingCount >= SINGLE_SERVICE_MAX_PENDING_ACKS) {
                        // Legacy single-service users need not ack. Bound memory,
                        // freeze progress and never skip an untracked record.
                        trackingOverflowed = true;
                        ((CheckpointManager) this).clearPendingRecords();
                        pendingCount = 0;
                        log.warn("DTS single-service pending acknowledgements exceeded {}. "
                                + "Recovery remains at the last confirmed boundary until consumer recreation.",
                                SINGLE_SERVICE_MAX_PENDING_ACKS);
                        return;
                    }
                    super.addRecord(record);
                    pendingCount++;
                }
            } finally {
                owner.lock.unlock();
            }
        }

        @Override
        public void removeRecord(Record record) {
            owner.lock.lock();
            try {
                if (owner.activeRecovery != this || trackingOverflowed || record.getPrev() == null) {
                    return;
                }
                super.removeRecord(record);
                pendingCount--;
                record.setPrev(null);
                record.setNext(null);
                String confirmed = getSaveCheckpoint();
                if (confirmed != null) {
                    owner.saveCheckpoint = confirmed;
                }
            } finally {
                owner.lock.unlock();
            }
        }
    }

}
