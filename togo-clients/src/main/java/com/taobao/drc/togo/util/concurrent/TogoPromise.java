package com.taobao.drc.togo.util.concurrent;

/**
 * @author yangyang
 * @since 17/3/21
 */
public interface TogoPromise<T> {
    void success(T t);

    void failure(Throwable throwable);

    TogoFuture<T> future();
}
