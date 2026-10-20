package com.tanmay.jobqueue.worker;

import java.net.InetAddress;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import com.tanmay.jobqueue.config.JobQueueProperties;
import com.tanmay.jobqueue.job.Job;
import com.tanmay.jobqueue.job.JobRepository;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Runs {@code concurrency} poller threads that each lease one job at a time,
 * plus a housekeeping thread that heartbeats in-flight jobs and returns jobs
 * with expired leases (crashed or hung workers) to the queue.
 */
@Component
public class JobWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JobWorker.class);
    private static final int MAX_ERROR_LENGTH = 2000;

    private final JobRepository repository;
    private final JobHandlerRegistry handlers;
    private final ObjectMapper objectMapper;
    private final JobQueueProperties props;
    private final Backoff backoff;
    private final String workerId;
    private final Set<Long> inFlight = ConcurrentHashMap.newKeySet();

    private volatile boolean running;
    private ExecutorService pollers;
    private ScheduledExecutorService housekeeping;

    public JobWorker(JobRepository repository, JobHandlerRegistry handlers,
                     ObjectMapper objectMapper, JobQueueProperties props) {
        this.repository = repository;
        this.handlers = handlers;
        this.objectMapper = objectMapper;
        this.props = props;
        this.backoff = new Backoff(props.backoffBase(), props.backoffMax());
        this.workerId = props.workerId() != null ? props.workerId() : defaultWorkerId();
    }

    @Override
    public void start() {
        if (!props.workerEnabled()) {
            log.info("Job worker disabled (jobqueue.worker-enabled=false)");
            return;
        }
        running = true;
        pollers = Executors.newFixedThreadPool(props.concurrency(),
                Thread.ofPlatform().name("job-poller-", 0).factory());
        for (int i = 0; i < props.concurrency(); i++) {
            pollers.submit(this::pollLoop);
        }

        housekeeping = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("job-housekeeping").daemon().factory());
        long heartbeatMillis = Math.max(props.visibilityTimeout().toMillis() / 3, 100);
        housekeeping.scheduleWithFixedDelay(this::heartbeat, heartbeatMillis, heartbeatMillis, TimeUnit.MILLISECONDS);
        long reaperMillis = props.reaperInterval().toMillis();
        housekeeping.scheduleWithFixedDelay(this::reap, reaperMillis, reaperMillis, TimeUnit.MILLISECONDS);

        log.info("Job worker {} started with {} pollers", workerId, props.concurrency());
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        pollers.shutdown();
        try {
            if (!pollers.awaitTermination(props.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("{} job(s) still running after shutdown timeout; their leases will expire and be retried",
                        inFlight.size());
                pollers.shutdownNow();
            }
        } catch (InterruptedException e) {
            pollers.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            housekeeping.shutdownNow();
        }
        log.info("Job worker {} stopped", workerId);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public String workerId() {
        return workerId;
    }

    private void pollLoop() {
        while (running) {
            try {
                List<Job> claimed = repository.claim(workerId, 1);
                if (claimed.isEmpty()) {
                    Thread.sleep(props.pollInterval());
                    continue;
                }
                claimed.forEach(this::execute);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // Usually the database is unreachable; back off instead of spinning.
                log.error("Polling failed", e);
                sleepQuietly();
            }
        }
    }

    void execute(Job job) {
        inFlight.add(job.id());
        try {
            JobHandler handler = handlers.find(job.type())
                    .orElseThrow(() -> new NonRetryableJobException("No handler registered for type '" + job.type() + "'"));
            JsonNode payload = objectMapper.readTree(job.payload());
            handler.handle(job, payload);
            if (!repository.markSucceeded(job.id(), workerId)) {
                log.warn("Job {} finished but its lease was lost; result not recorded", job.id());
            }
        } catch (Exception e) {
            onFailure(job, e);
        } finally {
            inFlight.remove(job.id());
        }
    }

    private void onFailure(Job job, Exception e) {
        String error = describe(e);
        boolean retryable = !(e instanceof NonRetryableJobException) && job.hasAttemptsLeft();
        if (retryable) {
            Instant retryAt = Instant.now().plus(backoff.delayAfter(job.attempts()));
            repository.reschedule(job.id(), workerId, error, retryAt);
            log.info("Job {} ({}) attempt {}/{} failed, retrying at {}: {}",
                    job.id(), job.type(), job.attempts(), job.maxAttempts(), retryAt, error);
        } else {
            repository.markDead(job.id(), workerId, error);
            log.warn("Job {} ({}) is DEAD after attempt {}/{}: {}",
                    job.id(), job.type(), job.attempts(), job.maxAttempts(), error);
        }
    }

    private void heartbeat() {
        try {
            repository.heartbeat(workerId, Set.copyOf(inFlight));
        } catch (RuntimeException e) {
            log.error("Heartbeat failed", e);
        }
    }

    private void reap() {
        try {
            int released = repository.releaseExpired(props.visibilityTimeout());
            if (released > 0) {
                log.warn("Released {} job(s) with expired leases", released);
            }
        } catch (RuntimeException e) {
            log.error("Reaper failed", e);
        }
    }

    private void sleepQuietly() {
        try {
            Thread.sleep(props.pollInterval());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String describe(Exception e) {
        String message = e.getClass().getSimpleName() + ": " + e.getMessage();
        return message.length() > MAX_ERROR_LENGTH ? message.substring(0, MAX_ERROR_LENGTH) : message;
    }

    private static String defaultWorkerId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "worker";
        }
        return host + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
