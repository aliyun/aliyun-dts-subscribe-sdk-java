/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A flexible future which supports call chaining and other asynchronous programming patterns. This will
 * eventually become a thin shim on top of Java 8's CompletableFuture.
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public abstract class KafkaFuture<T> implements Future<T> {
    /**
     * A function which takes objects of type A and returns objects of type B.
     */
    public static abstract class Function<A, B> {
        public abstract B apply(A a);
    }

    /**
     * A consumer of two different types of object.
     */
    public static abstract class BiConsumer<A, B> {
        public abstract void accept(A a, B b);
    }

    private static class AllOfAdapter<R> extends BiConsumer<R, Throwable> {
        private int remainingResponses;
        private KafkaFuture<?> future;

        public AllOfAdapter(int remainingResponses, KafkaFuture<?> future) {
            this.remainingResponses = remainingResponses;
            this.future = future;
            maybeComplete();
        }

        @Override
        public synchronized void accept(R newValue, Throwable exception) {
            if (remainingResponses <= 0)
                return;
            if (exception != null) {
                remainingResponses = 0;
                future.completeExceptionally(exception);
            } else {
                remainingResponses--;
                maybeComplete();
            }
        }

        private void maybeComplete() {
            if (remainingResponses <= 0)
                future.complete(null);
        }
    }

    /** 
     * Returns a new KafkaFuture that is already completed with the given value.
     */
    public static <U> KafkaFuture<U> completedFuture(U value) {
        KafkaFuture<U> future = new KafkaFutureImpl<U>();
        future.complete(value);
        return future;
    }

    /** 
     * Returns a new KafkaFuture that is completed when all the given futures have completed.  If
     * any future throws an exception, the returned future returns it.  If multiple futures throw
     * an exception, which one gets returned is arbitrarily chosen.
     */
    public static KafkaFuture<Void> allOf(KafkaFuture<?>... futures) {
        KafkaFuture<Void> allOfFuture = new KafkaFutureImpl<>();
        AllOfAdapter<Object> allOfWaiter = new AllOfAdapter<>(futures.length, allOfFuture);
        for (KafkaFuture<?> future : futures) {
            future.addWaiter(allOfWaiter);
        }
        return allOfFuture;
    }

    /**
     * Returns a new KafkaFuture that, when this future completes normally, is executed with this
     * futures's result as the argument to the supplied function.
     */
    public abstract <R> KafkaFuture<R> thenApply(Function<T, R> function);

    protected abstract void addWaiter(BiConsumer<? super T, ? super Throwable> action);

    /**
     * If not already completed, sets the value returned by get() and related methods to the given
     * value.
     */
    protected abstract boolean complete(T newValue);

    /**
     * If not already completed, causes invocations of get() and related methods to throw the given
     * exception.
     */
    protected abstract boolean completeExceptionally(Throwable newException);

    /**
     * If not already completed, completes this future with a CancellationException.  Dependent
     * futures that have not already completed will also complete exceptionally, with a
     * CompletionException caused by this CancellationException.
     */
    @Override
    public abstract boolean cancel(boolean mayInterruptIfRunning);

    /**
     * Waits if necessary for this future to complete, and then returns its result.
     */
    @Override
    public abstract T get() throws InterruptedException, ExecutionException;

    /**
     * Waits if necessary for at most the given time for this future to complete, and then returns
     * its result, if available.
     */
    @Override
    public abstract T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
        TimeoutException;

    /**
     * Returns the result value (or throws any encountered exception) if completed, else returns
     * the given valueIfAbsent.
     */
    public abstract T getNow(T valueIfAbsent) throws InterruptedException, ExecutionException;

    /**
     * Returns true if this CompletableFuture was cancelled before it completed normally.
     */
    @Override
    public abstract boolean isCancelled();

    /**
     * Returns true if this CompletableFuture completed exceptionally, in any way.
     */
    public abstract boolean isCompletedExceptionally();

    /**
     * Returns true if completed in any fashion: normally, exceptionally, or via cancellation.
     */
    @Override
    public abstract boolean isDone();
}
