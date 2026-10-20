package com.tanmay.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.tanmay.jobqueue.job.Job;
import com.tanmay.jobqueue.job.JobRepository;
import com.tanmay.jobqueue.job.JobStatus;
import com.tanmay.jobqueue.job.NewJob;

@SpringBootTest(properties = "jobqueue.worker-enabled=false")
class JobRepositoryTest extends PostgresTest {

    @Autowired
    JobRepository repository;

    @Test
    void concurrentWorkersNeverClaimTheSameJob() throws Exception {
        int jobCount = 300;
        int workers = 8;
        for (int i = 0; i < jobCount; i++) {
            repository.insert(job("demo.sleep", 0, Instant.now(), null));
        }

        CountDownLatch startTogether = new CountDownLatch(1);
        List<Callable<List<Long>>> tasks = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            String workerId = "worker-" + w;
            tasks.add(() -> {
                startTogether.await();
                List<Long> mine = new ArrayList<>();
                List<Job> batch;
                while (!(batch = repository.claim(workerId, 3)).isEmpty()) {
                    batch.forEach(j -> mine.add(j.id()));
                }
                return mine;
            });
        }

        List<Long> claimed = Collections.synchronizedList(new ArrayList<>());
        try (ExecutorService pool = Executors.newFixedThreadPool(workers)) {
            List<Future<List<Long>>> futures = tasks.stream().map(pool::submit).toList();
            startTogether.countDown();
            for (Future<List<Long>> f : futures) {
                claimed.addAll(f.get());
            }
        }

        assertThat(claimed).hasSize(jobCount).doesNotHaveDuplicates();
        assertThat(repository.countByStatus()).containsEntry(JobStatus.RUNNING, (long) jobCount);
    }

    @Test
    void claimsHigherPriorityFirstAndSkipsJobsScheduledInTheFuture() {
        Job low = repository.insert(job("a", 0, Instant.now(), null)).orElseThrow();
        Job high = repository.insert(job("b", 10, Instant.now(), null)).orElseThrow();
        repository.insert(job("c", 100, Instant.now().plusSeconds(3600), null));

        List<Job> claimed = repository.claim("w", 10);

        assertThat(claimed).extracting(Job::id).containsExactly(high.id(), low.id());
        assertThat(claimed).allSatisfy(j -> {
            assertThat(j.status()).isEqualTo(JobStatus.RUNNING);
            assertThat(j.attempts()).isEqualTo(1);
            assertThat(j.lockedBy()).isEqualTo("w");
        });
    }

    @Test
    void duplicateIdempotencyKeyIsNotInsertedTwice() {
        assertThat(repository.insert(job("a", 0, Instant.now(), "order-42"))).isPresent();
        assertThat(repository.insert(job("a", 0, Instant.now(), "order-42"))).isEmpty();
        assertThat(repository.findByIdempotencyKey("order-42")).isPresent();
    }

    @Test
    void workerThatLostItsLeaseCannotRecordAResult() {
        long id = repository.insert(job("a", 0, Instant.now(), null)).orElseThrow().id();
        repository.claim("original", 1);

        assertThat(repository.markSucceeded(id, "imposter")).isFalse();
        assertThat(repository.markDead(id, "imposter", "boom")).isFalse();
        assertThat(repository.markSucceeded(id, "original")).isTrue();
        assertThat(repository.findById(id).orElseThrow().status()).isEqualTo(JobStatus.SUCCEEDED);
    }

    @Test
    void rescheduledJobWaitsUntilItsRetryTime() {
        long id = repository.insert(job("a", 0, Instant.now(), null)).orElseThrow().id();
        repository.claim("w", 1);

        repository.reschedule(id, "w", "temporary failure", Instant.now().plusSeconds(60));

        Job job = repository.findById(id).orElseThrow();
        assertThat(job.status()).isEqualTo(JobStatus.QUEUED);
        assertThat(job.lastError()).isEqualTo("temporary failure");
        assertThat(job.lockedBy()).isNull();
        assertThat(repository.claim("w", 1)).isEmpty();
    }

    @Test
    void expiredLeasesAreReleasedAndExhaustedJobsGoDead() {
        long retryable = repository.insert(job("a", 0, Instant.now(), null)).orElseThrow().id();
        long exhausted = repository.insert(new NewJob("b", "{}", 0, 1, Instant.now(), null)).orElseThrow().id();
        repository.claim("crashed-worker", 2);
        jdbc.sql("update jobs set locked_at = now() - interval '10 minutes'").update();

        int released = repository.releaseExpired(Duration.ofMinutes(5));

        assertThat(released).isEqualTo(2);
        assertThat(repository.findById(retryable).orElseThrow().status()).isEqualTo(JobStatus.QUEUED);
        Job dead = repository.findById(exhausted).orElseThrow();
        assertThat(dead.status()).isEqualTo(JobStatus.DEAD);
        assertThat(dead.lastError()).contains("crashed-worker");
    }

    @Test
    void heartbeatKeepsALeaseAlive() {
        long id = repository.insert(job("a", 0, Instant.now(), null)).orElseThrow().id();
        repository.claim("w", 1);
        jdbc.sql("update jobs set locked_at = now() - interval '10 minutes'").update();

        repository.heartbeat("w", List.of(id));

        assertThat(repository.releaseExpired(Duration.ofMinutes(5))).isZero();
        assertThat(repository.findById(id).orElseThrow().status()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void deadJobCanBeRequeuedWithFreshAttempts() {
        long id = repository.insert(job("a", 0, Instant.now(), null)).orElseThrow().id();
        repository.claim("w", 1);
        repository.markDead(id, "w", "fatal");

        Job requeued = repository.requeueDead(id).orElseThrow();

        assertThat(requeued.status()).isEqualTo(JobStatus.QUEUED);
        assertThat(requeued.attempts()).isZero();
        assertThat(repository.requeueDead(id)).isEmpty();
    }

    private static NewJob job(String type, int priority, Instant runAt, String key) {
        return new NewJob(type, "{}", priority, 5, runAt, key);
    }
}
