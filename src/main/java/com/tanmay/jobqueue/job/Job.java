package com.tanmay.jobqueue.job;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonRawValue;

public record Job(
        long id,
        String type,
        @JsonRawValue String payload,
        JobStatus status,
        int priority,
        int attempts,
        int maxAttempts,
        Instant runAt,
        String lockedBy,
        Instant lockedAt,
        String lastError,
        String idempotencyKey,
        Instant createdAt,
        Instant updatedAt) {

    public boolean hasAttemptsLeft() {
        return attempts < maxAttempts;
    }
}
