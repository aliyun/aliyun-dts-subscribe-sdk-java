package com.taobao.drc.togo.util.concurrent;

import java.util.Objects;
import java.util.concurrent.*;

/**
 * @author yangyang
 * @since 17/5/3
 */
public class CompletableFutureTogoPromise<T> implements TogoPromise<T>, TogoFuture<T> {

    private final CompletableFuture<T> completableFuture = new CompletableFuture<>();

    @Override
    public void success(T t) {
        completableFuture.complete(t);
    }

    @Override
    public void failure(Throwable throwable) {
        completableFuture.completeExceptionally(throwable);
    }

    @Override
    public TogoFuture<T> future() {
        return this;
    }

    @Override
    public TogoFuture<T> addCallback(TogoCallback<T> callback, Executor executor) {
        Objects.requireNonNull(callback);
        completableFuture.whenCompleteAsync(callback::onComplete, executor);

        return this;
    }

    @Override
    public TogoFuture<T> addCallback(TogoCallback<T> callback) {
        Objects.requireNonNull(callback);
        completableFuture.whenComplete(callback::onComplete);

        return this;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return completableFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return completableFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return completableFuture.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return completableFuture.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return completableFuture.get(timeout, unit);
    }
}
