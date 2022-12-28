package com.aliyun.dts.subscribe.clients.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Utility methods for getting the time and computing intervals.
 */
public final class Time {

    /**
     * number of nano seconds in 1 millisecond.
     */
    private static final long NANOSECONDS_PER_MILLISECOND = 1000000;

    private static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");

    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
        new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSSZ");
            }
        };

    /**
     * Current system time.  Do not use this to calculate a duration or interval
     * to sleep, because it will be broken by settimeofday.  Instead, use
     * monotonicNow.
     *
     * @return current time in msec.
     */
    public static long now() {
        return System.currentTimeMillis();
    }

    /**
     * Current time from some arbitrary time base in the past, counting in
     * milliseconds, and not affected by settimeofday or similar system clock
     * changes.  This is appropriate to use when computing how much longer to
     * wait for an interval to expire.
     * This function can return a negative value and it must be handled correctly
     * by callers. See the documentation of System#nanoTime for caveats.
     *
     * @return a monotonic clock that counts in milliseconds.
     */
    public static long monotonicNow() {
        return System.nanoTime() / NANOSECONDS_PER_MILLISECOND;
    }

    /**
     * Same as {@link #monotonicNow()} but returns its result in nanoseconds.
     * Note that this is subject to the same resolution constraints as
     * {@link System#nanoTime()}.
     *
     * @return a monotonic clock that counts in nanoseconds.
     */
    public static long monotonicNowNanos() {
        return System.nanoTime();
    }

    /**
     * Convert time in millisecond to human readable format.
     * @param millis time
     * @return a human readable string for the input time
     */
    public static String formatTime(long millis) {
        return DATE_FORMAT.get().format(millis);
    }

    /**
     * Get the current UTC time in milliseconds.
     *
     * @return the current UTC time in milliseconds.
     */
    public static long getUtcTime() {
        return Calendar.getInstance(UTC_ZONE).getTimeInMillis();
    }
}
