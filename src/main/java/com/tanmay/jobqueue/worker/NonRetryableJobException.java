package com.tanmay.jobqueue.worker;

/** A failure that retrying cannot fix, such as an invalid payload. */
public class NonRetryableJobException extends RuntimeException {

    public NonRetryableJobException(String message) {
        super(message);
    }
}
