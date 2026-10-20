package com.tanmay.jobqueue.worker;

import com.tanmay.jobqueue.job.Job;

import tools.jackson.databind.JsonNode;

/**
 * Business logic for one job type. Register an implementation as a Spring bean
 * and jobs whose {@code type} matches {@link #type()} are routed to it.
 *
 * <p>Delivery is at-least-once: a job can run again if a worker dies after the
 * handler finished but before the result was saved, so handlers should be
 * idempotent.
 */
public interface JobHandler {

    String type();

    /**
     * Throw any exception to fail the attempt and retry with backoff, or
     * {@link NonRetryableJobException} to send the job straight to DEAD.
     */
    void handle(Job job, JsonNode payload) throws Exception;
}
