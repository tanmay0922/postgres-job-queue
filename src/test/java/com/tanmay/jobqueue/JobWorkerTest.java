package com.tanmay.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import com.tanmay.jobqueue.job.Job;
import com.tanmay.jobqueue.job.JobService;
import com.tanmay.jobqueue.job.JobStatus;

@SpringBootTest(properties = {
        "jobqueue.worker-enabled=true",
        "jobqueue.concurrency=4",
        "jobqueue.poll-interval=20ms",
        "jobqueue.backoff-base=20ms",
        "jobqueue.backoff-max=100ms",
})
// Closing the context stops the worker so it cannot claim jobs created by other test classes.
@DirtiesContext
class JobWorkerTest extends PostgresTest {

    @Autowired
    JobService jobs;

    @Test
    void processesJobsToCompletion() {
        long id = enqueue("demo.sleep", "{\"millis\": 10}", 3);

        Job job = awaitJob(id, j -> j.status() == JobStatus.SUCCEEDED);

        assertThat(job.attempts()).isEqualTo(1);
        assertThat(job.lockedBy()).isNull();
    }

    @Test
    void retriesWithBackoffUntilTheHandlerSucceeds() {
        long id = enqueue("demo.flaky", "{\"failTimes\": 2}", 5);

        Job job = awaitJob(id, j -> j.status() == JobStatus.SUCCEEDED);

        assertThat(job.attempts()).isEqualTo(3);
        assertThat(job.lastError()).isNull();
    }

    @Test
    void movesJobToDeadWhenAttemptsRunOut() {
        long id = enqueue("demo.flaky", "{\"failTimes\": 10}", 3);

        Job job = awaitJob(id, j -> j.status() == JobStatus.DEAD);

        assertThat(job.attempts()).isEqualTo(3);
        assertThat(job.lastError()).contains("Simulated failure on attempt 3");
    }

    @Test
    void unknownJobTypeIsNotRetried() {
        long id = enqueue("no.such.type", "{}", 5);

        Job job = awaitJob(id, j -> j.status() == JobStatus.DEAD);

        assertThat(job.attempts()).isEqualTo(1);
        assertThat(job.lastError()).contains("No handler registered");
    }

    private long enqueue(String type, String payload, int maxAttempts) {
        return jobs.enqueue(type, payload, 0, maxAttempts, 0, null).job().id();
    }

    private Job awaitJob(long id, Predicate<Job> condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        Job job = jobs.get(id);
        while (!condition.test(job)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for job " + id + "; last state: " + job);
            }
            sleep();
            job = jobs.get(id);
        }
        return job;
    }

    private static void sleep() {
        try {
            Thread.sleep(25);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
