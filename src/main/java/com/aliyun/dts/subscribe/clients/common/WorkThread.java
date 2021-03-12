package com.aliyun.dts.subscribe.clients.common;

import java.io.Closeable;

import static com.aliyun.dts.subscribe.clients.common.Util.swallowErrorClose;

public class WorkThread<T extends Runnable & Closeable> {
    private final T r;
    private final Thread worker;

    public WorkThread(T r, String name) {
        this.r = r;
        worker = new Thread(r);
        worker.setName(name);
    }

    public void start() {
        worker.start();
    }

    public void stop() {
        swallowErrorClose(r);
        try {
            worker.join(10000, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}