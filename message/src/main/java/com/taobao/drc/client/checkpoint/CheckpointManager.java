package com.taobao.drc.client.checkpoint;

import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.message.DataMessage.Record;

import java.util.concurrent.locks.ReentrantLock;

public class CheckpointManager {

    private volatile String saveCheckpoint;

    private Record header;

    private ReentrantLock lock;

    private boolean multiMode;

    private String initialRecoveryTimestamp;
    private Recovery activeRecovery;

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
            activeRecovery = new Recovery(this, getRecoveryTimestamp(initialTimestamp));
            return activeRecovery;
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

        private Recovery(CheckpointManager owner, String timestamp) {
            // Single-service listeners need not ack: do not accumulate a queue
            // for them. Without acknowledgement tracking recovery stays conservative.
            super(owner.isMultiMode());
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
                if (owner.activeRecovery == this && isMultiMode()) {
                    super.addRecord(record);
                }
            } finally {
                owner.lock.unlock();
            }
        }

        @Override
        public void removeRecord(Record record) {
            owner.lock.lock();
            try {
                if (owner.activeRecovery != this || record.getPrev() == null) {
                    return;
                }
                super.removeRecord(record);
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
