package com.tanmay.jobqueue.worker;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with "equal jitter": the delay for attempt {@code n} is
 * {@code min(max, base * 2^(n-1))}, of which the upper half is randomised so
 * jobs that failed together do not all retry at the same instant.
 */
public final class Backoff {

    private final Duration base;
    private final Duration max;

    public Backoff(Duration base, Duration max) {
        if (base.isNegative() || base.isZero() || max.compareTo(base) < 0) {
            throw new IllegalArgumentException("Need 0 < base <= max, got base=" + base + " max=" + max);
        }
        this.base = base;
        this.max = max;
    }

    /** Upper bound of the delay before retrying after the given (1-based) attempt. */
    public Duration ceiling(int attempt) {
        int exponent = Math.min(Math.max(attempt, 1) - 1, 30);
        long millis = base.toMillis() * (1L << exponent);
        return Duration.ofMillis(Math.min(millis, max.toMillis()));
    }

    public Duration delayAfter(int attempt) {
        long ceiling = ceiling(attempt).toMillis();
        long half = ceiling / 2;
        return Duration.ofMillis(half + ThreadLocalRandom.current().nextLong(ceiling - half + 1));
    }
}
