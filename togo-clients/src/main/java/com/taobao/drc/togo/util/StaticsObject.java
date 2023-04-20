package com.taobao.drc.togo.util;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by longxuan on 18/1/9.
 */
public abstract class StaticsObject<T> {
    public volatile long lastAccessTimeMs;
    public long expireTimeMs;
    public ReentrantLock lock = new ReentrantLock();
    public abstract T value();
    public abstract String serialize(T value);
    public abstract T copyElement();
    public abstract void processValue(T value);
    public StaticsObject(long expireTimeMs) {
        this.expireTimeMs = expireTimeMs;
        lastAccessTimeMs = System.currentTimeMillis();
    }
    public void process(T v) {
        lock.lock();
        try {
            processValue(v);
        } finally {
            lock.unlock();
        }
        lastAccessTimeMs = System.currentTimeMillis();
    }

    public boolean hasExpired() {
        return System.currentTimeMillis() - lastAccessTimeMs > expireTimeMs;
    }

    public String getSerializedString() {
        T lastMirror = null;
        lock.lock();
        try {
            lastMirror = copyElement();
        } finally {
            lock.unlock();
        }
        return serialize(lastMirror);
    }
}
