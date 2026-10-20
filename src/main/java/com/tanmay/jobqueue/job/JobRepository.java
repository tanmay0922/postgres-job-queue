package com.tanmay.jobqueue.job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * All SQL for the queue lives here. Every state change from RUNNING checks
 * {@code locked_by}, so a worker whose lease expired cannot overwrite the
 * outcome recorded by the worker that took the job over.
 */
@Repository
public class JobRepository {

    private static final RowMapper<Job> JOB = JobRepository::mapJob;

    private final JdbcClient jdbc;

    public JobRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts the job, or returns empty if its idempotency key already exists. */
    public Optional<Job> insert(NewJob job) {
        return jdbc.sql("""
                        insert into jobs (type, payload, priority, max_attempts, run_at, idempotency_key)
                        values (:type, cast(:payload as jsonb), :priority, :maxAttempts, :runAt, :key)
                        on conflict (idempotency_key) do nothing
                        returning *
                        """)
                .param("type", job.type())
                .param("payload", job.payload())
                .param("priority", job.priority())
                .param("maxAttempts", job.maxAttempts())
                .param("runAt", toOffset(job.runAt()))
                .param("key", job.idempotencyKey())
                .query(JOB)
                .optional();
    }

    /**
     * Atomically leases up to {@code limit} ready jobs to {@code workerId}.
     * {@code SKIP LOCKED} lets concurrent workers pass over rows another
     * transaction is claiming instead of blocking on them, so no job is
     * handed out twice and workers never queue up behind each other.
     */
    public List<Job> claim(String workerId, int limit) {
        return jdbc.sql("""
                        update jobs j
                           set status = 'RUNNING', locked_by = :worker, locked_at = now(),
                               attempts = j.attempts + 1, updated_at = now()
                          from (select id from jobs
                                 where status = 'QUEUED' and run_at <= now()
                                 order by priority desc, run_at, id
                                 limit :limit
                                 for update skip locked) ready
                         where j.id = ready.id
                        returning j.*
                        """)
                .param("worker", workerId)
                .param("limit", limit)
                .query(JOB)
                .list();
    }

    public boolean markSucceeded(long id, String workerId) {
        return jdbc.sql("""
                        update jobs
                           set status = 'SUCCEEDED', locked_by = null, locked_at = null,
                               last_error = null, updated_at = now()
                         where id = :id and status = 'RUNNING' and locked_by = :worker
                        """)
                .param("id", id)
                .param("worker", workerId)
                .update() == 1;
    }

    public boolean reschedule(long id, String workerId, String error, Instant runAt) {
        return jdbc.sql("""
                        update jobs
                           set status = 'QUEUED', run_at = :runAt, last_error = :error,
                               locked_by = null, locked_at = null, updated_at = now()
                         where id = :id and status = 'RUNNING' and locked_by = :worker
                        """)
                .param("id", id)
                .param("worker", workerId)
                .param("error", error)
                .param("runAt", toOffset(runAt))
                .update() == 1;
    }

    public boolean markDead(long id, String workerId, String error) {
        return jdbc.sql("""
                        update jobs
                           set status = 'DEAD', last_error = :error,
                               locked_by = null, locked_at = null, updated_at = now()
                         where id = :id and status = 'RUNNING' and locked_by = :worker
                        """)
                .param("id", id)
                .param("worker", workerId)
                .param("error", error)
                .update() == 1;
    }

    /** Extends the lease on jobs this worker is still processing. */
    public int heartbeat(String workerId, Collection<Long> ids) {
        if (ids.isEmpty()) {
            return 0;
        }
        return jdbc.sql("""
                        update jobs set locked_at = now()
                         where id in (:ids) and status = 'RUNNING' and locked_by = :worker
                        """)
                .param("ids", ids)
                .param("worker", workerId)
                .update();
    }

    /**
     * Returns jobs whose lease expired (the worker crashed or hung) to the
     * queue, or marks them dead if they have no attempts left.
     */
    public int releaseExpired(Duration visibilityTimeout) {
        return jdbc.sql("""
                        update jobs
                           set status = case when attempts >= max_attempts then 'DEAD' else 'QUEUED' end,
                               last_error = 'lease expired on worker ' || locked_by,
                               run_at = now(), locked_by = null, locked_at = null, updated_at = now()
                         where status = 'RUNNING'
                           and locked_at < now() - make_interval(secs => :seconds)
                        """)
                .param("seconds", visibilityTimeout.toMillis() / 1000.0)
                .update();
    }

    /** Puts a dead job back in the queue with a fresh set of attempts. */
    public Optional<Job> requeueDead(long id) {
        return jdbc.sql("""
                        update jobs
                           set status = 'QUEUED', attempts = 0, run_at = now(),
                               last_error = null, updated_at = now()
                         where id = :id and status = 'DEAD'
                        returning *
                        """)
                .param("id", id)
                .query(JOB)
                .optional();
    }

    public Optional<Job> findById(long id) {
        return jdbc.sql("select * from jobs where id = :id")
                .param("id", id)
                .query(JOB)
                .optional();
    }

    public Optional<Job> findByIdempotencyKey(String key) {
        return jdbc.sql("select * from jobs where idempotency_key = :key")
                .param("key", key)
                .query(JOB)
                .optional();
    }

    public List<Job> findByStatus(JobStatus status, int limit) {
        return jdbc.sql("select * from jobs where status = :status order by id desc limit :limit")
                .param("status", status.name())
                .param("limit", limit)
                .query(JOB)
                .list();
    }

    public Map<JobStatus, Long> countByStatus() {
        Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
        for (JobStatus status : JobStatus.values()) {
            counts.put(status, 0L);
        }
        jdbc.sql("select status, count(*) as n from jobs group by status")
                .query((rs, row) -> Map.entry(JobStatus.valueOf(rs.getString("status")), rs.getLong("n")))
                .list()
                .forEach(e -> counts.put(e.getKey(), e.getValue()));
        return counts;
    }

    private static Job mapJob(ResultSet rs, int row) throws SQLException {
        return new Job(
                rs.getLong("id"),
                rs.getString("type"),
                rs.getString("payload"),
                JobStatus.valueOf(rs.getString("status")),
                rs.getInt("priority"),
                rs.getInt("attempts"),
                rs.getInt("max_attempts"),
                instant(rs, "run_at"),
                rs.getString("locked_by"),
                instant(rs, "locked_at"),
                rs.getString("last_error"),
                rs.getString("idempotency_key"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static OffsetDateTime toOffset(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }
}
