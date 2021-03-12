package com.aliyun.dts.subscribe.clients.common.function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.function.Function;

//@SuppressFBWarnings
public final class SwallowException<T extends Closeable> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SwallowException.class);

    private final T o;

    private SwallowException(T oo) {
        this.o = oo;
    }

    public interface CalleeFunctionWithoutReturnValue {
        void call() throws Exception;
    }

    public interface CalleeFunctionWithReturnValue<R> {
        R call() throws Exception;
    }

    public interface ConsumerFunctionWithoutReturnValue<L> {
        void call(L value) throws Exception;
    }

    public static <R> R callAndSwallowException(CalleeFunctionWithReturnValue<R> callee) {
        try {
            return callee.call();
        } catch (Exception e) {
            LOGGER.info("call function {} failed, and swallow exception", callee.getClass().getName(), e);
        }

        return null;
    }

    public static void callAndSwallowException(CalleeFunctionWithoutReturnValue function) {
        callAndSwallowException((CalleeFunctionWithReturnValue<Void>) () -> {
            function.call();
            return null;
        });
    }

    public static <R> R callAndThrowRuntimeException(CalleeFunctionWithReturnValue<R> callee) {
        try {
            return callee.call();
        } catch (Exception e) {
            LOGGER.info("call function {} failed, and swallow exception", callee.getClass().getName(), e);
            throw new RuntimeException(e.toString(), e);
        }
    }

    public static void callAndThrowRuntimeException(CalleeFunctionWithoutReturnValue function) {
        callAndThrowRuntimeException((CalleeFunctionWithReturnValue<Void>) () -> {
            function.call();
            return null;
        });
    }

    public static <R, T extends RuntimeException> R callAndThrowException(CalleeFunctionWithReturnValue<R> callee,
                                                                          Function<Exception, T> exceptionSupplier) {
        try {
            return callee.call();
        } catch (Exception e) {
            LOGGER.info("call function {} failed", callee.getClass().getName(), e);
            throw exceptionSupplier.apply(e);
        }
    }

    public static <T extends RuntimeException> void callAndThrowException(CalleeFunctionWithoutReturnValue function,
                                                                          Function<Exception, T> exceptionSupplier) {
        callAndThrowException((CalleeFunctionWithReturnValue<Void>) () -> {
            function.call();
            return null;
        }, exceptionSupplier);
    }

    public static <R extends Closeable> SwallowException<R> create(R oo) {
        return new SwallowException<>(oo);
    }

    public T getObject() {
        return o;
    }

    @Override
    public void close() {
        callAndSwallowException(() -> o.close());
    }
}
