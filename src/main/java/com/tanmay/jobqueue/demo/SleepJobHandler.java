package com.tanmay.jobqueue.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.tanmay.jobqueue.job.Job;
import com.tanmay.jobqueue.worker.JobHandler;

import tools.jackson.databind.JsonNode;

/** Example handler: simulates work. Payload: {@code {"millis": 250}}. */
@Component
class SleepJobHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(SleepJobHandler.class);

    @Override
    public String type() {
        return "demo.sleep";
    }

    @Override
    public void handle(Job job, JsonNode payload) throws InterruptedException {
        long millis = payload.path("millis").asLong(100);
        Thread.sleep(millis);
        log.info("Job {} slept {} ms", job.id(), millis);
    }
}
