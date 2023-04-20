package com.taobao.drc.togo.util.concurrent;

import java.util.concurrent.Executor;

/**
 * @author yangyang
 * @since 17/3/21
 */
public class TogoFutures {
    public static <T> TogoPromise<T> promise() {
        return new CompletableFutureTogoPromise<>();
    }

    public static <T> TogoFuture<T> success(T t) {
        TogoPromise<T> promise = promise();
        promise.success(t);
        return promise.future();
    }

    public static <T> TogoFuture<T> failure(Throwable throwable) {
        TogoPromise<T> promise = promise();
        promise.failure(throwable);
        return promise.future();
    }

    private static final Executor directExecutor = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };

    public static Executor directExecutor() {
        return directExecutor;
    }
}
