package com.tanmay.jobqueue.job;

public enum JobStatus {
    /** Waiting to run once {@code run_at} has passed. */
    QUEUED,
    /** Claimed by a worker that holds the lease. */
    RUNNING,
    /** Finished successfully. */
    SUCCEEDED,
    /** Out of attempts or failed permanently; needs a manual retry. */
    DEAD
}
