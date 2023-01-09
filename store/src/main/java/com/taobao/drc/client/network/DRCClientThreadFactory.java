package com.taobao.drc.client.network;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jianjundeng on 8/8/16.
 */
public class DRCClientThreadFactory implements ThreadFactory {

    private final AtomicInteger threadNumber = new AtomicInteger(1);

    private String prefix;

    public DRCClientThreadFactory(String prefix){
        this.prefix=prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(null, r,
                prefix +"-"+ threadNumber.getAndIncrement(),
                0);
        return t;
    }
}
