create table jobs (
    id              bigserial    primary key,
    type            varchar(100) not null,
    payload         jsonb        not null default '{}'::jsonb,
    status          varchar(16)  not null default 'QUEUED',
    priority        integer      not null default 0,
    attempts        integer      not null default 0,
    max_attempts    integer      not null default 5,
    run_at          timestamptz  not null default now(),
    locked_by       varchar(100),
    locked_at       timestamptz,
    last_error      text,
    idempotency_key varchar(200) unique,
    created_at      timestamptz  not null default now(),
    updated_at      timestamptz  not null default now(),
    constraint jobs_status_check check (status in ('QUEUED', 'RUNNING', 'SUCCEEDED', 'DEAD')),
    constraint jobs_attempts_check check (max_attempts > 0 and attempts >= 0)
);

-- Partial index that serves the claim query: only ready-to-run rows, in pick order.
create index jobs_ready_idx on jobs (priority desc, run_at, id) where status = 'QUEUED';

-- Lets the reaper find expired leases without scanning finished jobs.
create index jobs_running_idx on jobs (locked_at) where status = 'RUNNING';
