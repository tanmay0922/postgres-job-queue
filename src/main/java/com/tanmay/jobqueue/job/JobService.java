package com.tanmay.jobqueue.job;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

@Service
public class JobService {

    private final JobRepository repository;

    public JobService(JobRepository repository) {
        this.repository = repository;
    }

    /**
     * Enqueues a job. When an idempotency key is given and a job with that key
     * already exists, the existing job is returned instead of a duplicate.
     */
    public EnqueueResult enqueue(String type, String payload, int priority, int maxAttempts,
                                 long delaySeconds, String idempotencyKey) {
        Instant runAt = Instant.now().plusSeconds(delaySeconds);
        NewJob job = new NewJob(type, payload, priority, maxAttempts, runAt, idempotencyKey);
        return repository.insert(job)
                .map(created -> new EnqueueResult(created, true))
                .orElseGet(() -> new EnqueueResult(existing(idempotencyKey), false));
    }

    public Job get(long id) {
        return repository.findById(id).orElseThrow(() -> new JobNotFoundException(id));
    }

    public List<Job> list(JobStatus status, int limit) {
        return repository.findByStatus(status, limit);
    }

    public Map<JobStatus, Long> stats() {
        return repository.countByStatus();
    }

    public Job retry(long id) {
        return repository.requeueDead(id).orElseThrow(() -> {
            Job job = get(id);
            return new IllegalStateException("Job " + id + " is " + job.status() + "; only DEAD jobs can be retried");
        });
    }

    private Job existing(String idempotencyKey) {
        // insert() only returns empty on an idempotency-key conflict, so the key is non-null here.
        return repository.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> new IllegalStateException("Conflicting job for key " + idempotencyKey + " vanished"));
    }

    public record EnqueueResult(Job job, boolean created) {
    }
}
