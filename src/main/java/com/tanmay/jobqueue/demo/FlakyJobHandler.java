package com.tanmay.jobqueue.demo;

import org.springframework.stereotype.Component;

import com.tanmay.jobqueue.job.Job;
import com.tanmay.jobqueue.worker.JobHandler;

import tools.jackson.databind.JsonNode;

/**
 * Example handler that fails its first {@code failTimes} attempts, to show
 * retries and backoff. Payload: {@code {"failTimes": 2}}.
 */
@Component
class FlakyJobHandler implements JobHandler {

    @Override
    public String type() {
        return "demo.flaky";
    }

    @Override
    public void handle(Job job, JsonNode payload) {
        int failTimes = payload.path("failTimes").asInt(1);
        if (job.attempts() <= failTimes) {
            throw new IllegalStateException("Simulated failure on attempt " + job.attempts());
        }
    }
}
