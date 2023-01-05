package com.taobao.drc.client.network;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.RejectedExecutionHandlers;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by jianjundeng on 8/8/16.
 */
public class DRCClientEventExecutorGroup extends DefaultEventExecutorGroup {
    public DRCClientEventExecutorGroup(int threads,ThreadFactory threadFactory) {
        super(threads==0?Runtime.getRuntime().availableProcessors()*2:threads, threadFactory, 1024,
                RejectedExecutionHandlers.backoff(Integer.MAX_VALUE, 10, TimeUnit.MILLISECONDS));
    }
}
