package com.taobao.drc.togo.util.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * @author yangyang
 * @since 17/3/8
 */
public interface TogoFuture<T> extends Future<T> {

    TogoFuture<T> addCallback(TogoCallback<T> callback, Executor executor);

    TogoFuture<T> addCallback(TogoCallback<T> callback);
}
