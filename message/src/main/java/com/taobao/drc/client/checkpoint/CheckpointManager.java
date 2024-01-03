package com.taobao.drc.client.checkpoint;

import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.message.DataMessage.Record;

import java.util.concurrent.locks.ReentrantLock;

public class CheckpointManager {

    private volatile String saveCheckpoint;

    private Record header;

    private ReentrantLock lock;

    private boolean multiMode;

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

}
