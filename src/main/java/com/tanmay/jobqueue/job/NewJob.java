package com.tanmay.jobqueue.job;

import java.time.Instant;

/** A job to insert. {@code payload} must be a JSON document. */
public record NewJob(
        String type,
        String payload,
        int priority,
        int maxAttempts,
        Instant runAt,
        String idempotencyKey) {
}
