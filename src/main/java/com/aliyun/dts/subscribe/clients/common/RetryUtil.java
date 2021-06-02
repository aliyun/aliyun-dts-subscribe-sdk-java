package com.aliyun.dts.subscribe.clients.common;

import com.aliyun.dts.subscribe.clients.exception.CriticalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class RetryUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RetryUtil.class);
    private final String globalJobType;
    private final String objectNameShouldBeRetried;
    private Function<Throwable, Boolean> recoverableChecker;
    private int maxRetryTimes;
    private int freezeInterval;
    private TimeUnit freezeTimeUnit;

    public RetryUtil(String globalJobType, String objectNameShouldBeRetried,
                     int freezeInterval, TimeUnit freezeTimeUnit, int maxRetryTimes,
                     Function<Throwable, Boolean> recoverableChecker) {
        this.globalJobType = globalJobType;
        this.objectNameShouldBeRetried = objectNameShouldBeRetried;
        this.maxRetryTimes = maxRetryTimes;
        this.freezeInterval = freezeInterval;
        this.freezeTimeUnit = freezeTimeUnit;
        this.recoverableChecker = recoverableChecker;
    }

    public RetryUtil(Function<Throwable, Boolean> recoverableChecker) {
        this(5, TimeUnit.SECONDS, 1, recoverableChecker);
    }

    public RetryUtil(int freezeInterval, TimeUnit freezeTimeUnit, int maxRetryTimes,
                     Function<Throwable, Boolean> recoverableChecker) {
        this("unknown", "unknown", freezeInterval, freezeTimeUnit, maxRetryTimes, recoverableChecker);
    }

    public void callFunctionWithRetry(ThrowableFunctionVoid throwableFunction) throws Exception {
        callFunctionWithRetry(throwableFunction, null, null);
    }

    public void callFunctionWithRetry(ThrowableFunctionVoid throwableFunction,
                                      BiFunction<Throwable, Integer, Boolean> recoverableChecker,
                                      Consumer<RetryInfo> retryInfoConsumer) throws Exception {
        callFunctionWithRetry(
                () -> {
                    throwableFunction.call();
                    return null;
                },
                recoverableChecker, retryInfoConsumer);
    }

    public <T> T callFunctionWithRetry(ThrowableFunction<T> throwableFunction) throws Exception {
        return callFunctionWithRetry(maxRetryTimes, freezeInterval, freezeTimeUnit, throwableFunction);
    }

    public <T> T callFunctionWithRetry(int maxRetryTimes, int freezeInternal, TimeUnit freezeTimeUnit,
                                       ThrowableFunction<T> throwableFunction) throws Exception {
        return this.callFunctionWithRetry(maxRetryTimes, freezeInternal, freezeTimeUnit, throwableFunction,
                (e, times) -> recoverableChecker.apply(e), null);
    }

    public <T> T callFunctionWithRetry(ThrowableFunction<T> throwableFunction,
                                       BiFunction<Throwable, Integer, Boolean> recoverableChecker,
                                       Consumer<RetryInfo> retryInfoConsumer) throws Exception {
        return callFunctionWithRetry(maxRetryTimes, freezeInterval, freezeTimeUnit, throwableFunction, recoverableChecker, retryInfoConsumer);
    }

    public <T> T callFunctionWithRetry(int maxRetryTimes, int freezeInternal, TimeUnit freezeTimeUnit,
                                       ThrowableFunction<T> throwableFunction,
                                       BiFunction<Throwable, Integer, Boolean> recoverableChecker,
                                       Consumer<RetryInfo> retryInfoConsumer) throws Exception {
        Throwable error = null;
        RetryInfo retryInfo = null;
        maxRetryTimes = Math.max(1, maxRetryTimes);

        for (int i = 0; i < maxRetryTimes; i++) {
            try {
                T rs = throwableFunction.call();
                if (null != retryInfo) {
                    retryInfo.endRetry();
                }
                return rs;
            } catch (Throwable e) {
                error = e;
                if (recoverableChecker.apply(e, i)) {
                    if (null == retryInfo) {
                        retryInfo = new RetryInfo(globalJobType, objectNameShouldBeRetried);
                    }
                    retryInfo.retry(e);

                    LOG.warn("call function {} with {} times failed, try to execute it again",
                            throwableFunction.toString(), retryInfo.getRetryCount(), e);
                    freezeTimeUnit.sleep(freezeInternal);

                    if (null != retryInfoConsumer) {
                        retryInfoConsumer.accept(retryInfo);
                    }
                } else {
                    break;
                }
            }
        }

        if (error instanceof Exception) {
            throw (Exception) error;
        } else {
            throw new CriticalException("common", error);
        }
    }

    public interface ThrowableFunctionVoid {
        void call() throws Exception;
    }

    public interface ThrowableFunction<T> {
        T call() throws Exception;
    }

    public static class RetryInfo {
        private final String retryModule;
        private final String retryTarget;
        private String errMsg;

        private long retryCount;
        private long beginTimestamp;
        private long endTimestamp;

        public RetryInfo(String retryModule, String retryTarget) {
            this.retryModule = retryModule;
            this.retryTarget = retryTarget;
            this.retryCount = 0;
            this.errMsg = "null";
            this.beginTimestamp = 0;
            this.endTimestamp = 0;
        }

        public boolean isRetrying() {
            return 0 != beginTimestamp && 0 == endTimestamp;
        }

        public long getBeginTimestamp() {
            return beginTimestamp;
        }

        void beginRetry() {
            this.beginTimestamp = Time.now();
        }

        void endRetry() {
            this.endTimestamp = Time.now();
        }

        public long getRetryCount() {
            return retryCount;
        }

        public void retry(Throwable e) {
            if (0 == beginTimestamp) {
                beginRetry();
            }
            if (null != e) {
                errMsg = e.toString();
            }
            retryCount++;
        }

        public String getRetryModule() {
            return retryModule;
        }

        public String getErrMsg() {
            return errMsg;
        }

        void setErrMsg(String errMsg) {
            this.errMsg = errMsg;
        }

        public String getRetryTarget() {
            return retryTarget;
        }

        public long getRetryTime(TimeUnit unit) {
            long end = (0 == endTimestamp) ? Time.now() : endTimestamp;
            return TimeUnit.MILLISECONDS.convert(end - beginTimestamp, unit);
        }
    }

}
