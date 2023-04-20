package com.taobao.drc.togo.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * @author yangyang
 * @since 17/3/21
 */
public class DefaultTogoPromise<T> implements TogoPromise<T>, TogoFuture<T> {
    private FutureTask<T> futureTask;
    private List<CallbackEntry<T>> callbacks = null;
    private volatile T result = null;
    private volatile Throwable throwable = null;

    public DefaultTogoPromise() {
        this.futureTask = new FutureTask<T>(new Callable<T>() {
            @Override
            public T call() throws Exception {
                if (throwable != null) {
                    if (throwable instanceof Exception) {
                        throw (Exception)throwable;
                    } else {
                        throw new ExecutionException(throwable);
                    }
                } else {
                    return result;
                }
            }
        });
    }

    @Override
    public void success(T t) {
        synchronized (this) {
            if (!isCompleted()) {
                result = t;
                setCompleted();
            }
        }
    }

    @Override
    public void failure(Throwable throwable) {
        Objects.requireNonNull(throwable);
        synchronized (this) {
            if (!isCompleted()) {
                this.throwable = throwable;
                setCompleted();
            }
        }
    }

    @Override
    public TogoFuture<T> future() {
        return this;
    }

    private void setCompleted() {
        if (!isCompleted()) {
            futureTask.run();
            if (callbacks != null) {
                for (CallbackEntry<T> entry : callbacks) {
                    runCallback(entry.getCallback(), entry.getExecutor());
                }
            }
        }
    }

    private boolean isCompleted() {
        synchronized (this) {
            return futureTask.isDone();
        }
    }

    private void addListenerHelper(TogoCallback<T> callback, Executor executor) {
        if (callbacks == null) {
            callbacks = new ArrayList<>();
        }
        callbacks.add(new CallbackEntry<T>(callback, executor));
    }

    private void runCallback(final TogoCallback<T> callback, Executor executor) {
        if (!futureTask.isDone())
            throw new IllegalStateException("To run callbacks while the future task is not done");

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                if (throwable != null) {
                    callback.onComplete(null, throwable);
                } else if (futureTask.isCancelled()) {
                    callback.onComplete(null, new CancellationException());
                } else {
                    callback.onComplete(result, null);
                }
            }
        };

        if (null == executor) {
            runnable.run();
        } else {
            executor.execute(runnable);
        }
    }

    @Override
    public DefaultTogoPromise<T> addCallback(TogoCallback<T> callback, Executor executor) {
        synchronized (this) {
            if (isCompleted()) {
                runCallback(callback, executor);
            } else {
                addListenerHelper(callback, executor);
            }
        }
        return this;
    }

    @Override
    public DefaultTogoPromise<T> addCallback(TogoCallback<T> callback) {
        return addCallback(callback, null);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return futureTask.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return futureTask.isCancelled();
    }

    @Override
    public boolean isDone() {
        return futureTask.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return futureTask.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return futureTask.get(timeout, unit);
    }

    static class CallbackEntry<T> {
        private final TogoCallback<T> callback;
        private final Executor executor;

        public CallbackEntry(TogoCallback<T> callback, Executor executor) {
            this.callback = callback;
            this.executor = executor;
        }

        public TogoCallback<T> getCallback() {
            return callback;
        }

        public Executor getExecutor() {
            return executor;
        }
    }
}
