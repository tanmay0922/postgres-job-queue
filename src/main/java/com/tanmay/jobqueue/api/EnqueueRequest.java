package com.tanmay.jobqueue.api;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import tools.jackson.databind.JsonNode;

public record EnqueueRequest(
        @NotBlank @Size(max = 100) @Pattern(regexp = "[a-zA-Z0-9._-]+") String type,
        JsonNode payload,
        @Min(-100) @Max(100) Integer priority,
        @Min(1) @Max(50) Integer maxAttempts,
        @PositiveOrZero @Max(31_536_000) Long delaySeconds,
        @Size(max = 200) String idempotencyKey) {

    String payloadJson() {
        return payload == null || payload.isNull() ? "{}" : payload.toString();
    }
}
