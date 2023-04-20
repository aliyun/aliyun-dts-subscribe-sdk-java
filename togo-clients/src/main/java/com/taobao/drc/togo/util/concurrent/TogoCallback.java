package com.taobao.drc.togo.util.concurrent;

/**
 * @author yangyang
 * @since 17/3/8
 */
public interface TogoCallback<T> {
    void onComplete(T t, Throwable throwable);
}
