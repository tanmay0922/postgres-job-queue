package com.tanmay.jobqueue.job;

public class JobNotFoundException extends RuntimeException {

    public JobNotFoundException(long id) {
        super("Job " + id + " not found");
    }
}
