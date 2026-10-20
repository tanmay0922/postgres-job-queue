package com.tanmay.jobqueue.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class BackoffTest {

    private final Backoff backoff = new Backoff(Duration.ofSeconds(2), Duration.ofMinutes(1));

    @Test
    void ceilingDoublesEachAttemptUpToTheMax() {
        assertThat(backoff.ceiling(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(backoff.ceiling(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(backoff.ceiling(5)).isEqualTo(Duration.ofSeconds(32));
        assertThat(backoff.ceiling(6)).isEqualTo(Duration.ofMinutes(1));
        assertThat(backoff.ceiling(1_000)).isEqualTo(Duration.ofMinutes(1));
    }

    @RepeatedTest(50)
    void delayStaysBetweenHalfTheCeilingAndTheCeiling() {
        Duration delay = backoff.delayAfter(3);
        assertThat(delay).isBetween(Duration.ofSeconds(4), Duration.ofSeconds(8));
    }

    @Test
    void rejectsInvalidBounds() {
        assertThatThrownBy(() -> new Backoff(Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Backoff(Duration.ofSeconds(5), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
