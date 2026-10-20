package com.tanmay.jobqueue.api;

import java.net.URI;
import java.util.List;
import java.util.Map;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.tanmay.jobqueue.job.Job;
import com.tanmay.jobqueue.job.JobService;
import com.tanmay.jobqueue.job.JobService.EnqueueResult;
import com.tanmay.jobqueue.job.JobStatus;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobs;

    public JobController(JobService jobs) {
        this.jobs = jobs;
    }

    /** 201 for a new job; 200 with the original job when the idempotency key was seen before. */
    @PostMapping
    public ResponseEntity<Job> enqueue(@Valid @RequestBody EnqueueRequest request) {
        EnqueueResult result = jobs.enqueue(
                request.type(),
                request.payloadJson(),
                request.priority() == null ? 0 : request.priority(),
                request.maxAttempts() == null ? 5 : request.maxAttempts(),
                request.delaySeconds() == null ? 0 : request.delaySeconds(),
                request.idempotencyKey());
        Job job = result.job();
        return result.created()
                ? ResponseEntity.created(URI.create("/api/jobs/" + job.id())).body(job)
                : ResponseEntity.ok(job);
    }

    @GetMapping("/{id}")
    public Job get(@PathVariable long id) {
        return jobs.get(id);
    }

    @GetMapping
    public List<Job> list(@RequestParam(defaultValue = "QUEUED") JobStatus status,
                          @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
        return jobs.list(status, limit);
    }

    @GetMapping("/stats")
    public Map<JobStatus, Long> stats() {
        return jobs.stats();
    }

    @PostMapping("/{id}/retry")
    public Job retry(@PathVariable long id) {
        return jobs.retry(id);
    }
}
