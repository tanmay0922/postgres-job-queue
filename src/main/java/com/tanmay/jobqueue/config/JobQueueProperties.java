package com.tanmay.jobqueue.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Tuning knobs for the queue and its workers.
 *
 * @param workerEnabled     run worker threads in this instance (an API-only node can turn this off)
 * @param concurrency       number of jobs this instance processes at once
 * @param pollInterval      how long an idle worker waits before polling again
 * @param visibilityTimeout how long a claimed job may go without a heartbeat before it is reclaimed
 * @param backoffBase       delay before the first retry; doubles on each further attempt
 * @param backoffMax        upper bound for any single retry delay
 * @param reaperInterval    how often expired leases are swept back to the queue
 * @param shutdownTimeout   how long shutdown waits for in-flight jobs
 * @param workerId          identity written to {@code locked_by}; defaults to host name + random suffix
 */
@ConfigurationProperties("jobqueue")
public record JobQueueProperties(
        @DefaultValue("true") boolean workerEnabled,
        @DefaultValue("4") int concurrency,
        @DefaultValue("500ms") Duration pollInterval,
        @DefaultValue("5m") Duration visibilityTimeout,
        @DefaultValue("2s") Duration backoffBase,
        @DefaultValue("10m") Duration backoffMax,
        @DefaultValue("30s") Duration reaperInterval,
        @DefaultValue("30s") Duration shutdownTimeout,
        String workerId) {
}
