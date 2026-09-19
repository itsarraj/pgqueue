-- Recurring/cron job definitions. This does not replace `pgqueue_jobs` — a
-- recurring definition is a template that periodically gets materialized
-- into an ordinary row in `pgqueue_jobs` (via `Queue::tick_recurring`,
-- driven by `scheduler::run_scheduler`), which then goes through the exact
-- same claim/dispatch/retry/dead-letter machinery as any other job. The
-- "next run" bookkeeping lives here, as `next_run_at` on the definition
-- itself, rather than trying to derive it on the fly from the cron
-- expression on every scheduler tick — this way a tick is just "find rows
-- due now" (an indexed range scan), and next_run_at is only recomputed once,
-- at the moment a definition actually fires.
CREATE TABLE IF NOT EXISTS pgqueue_recurring_jobs (
    id BIGSERIAL PRIMARY KEY,
    -- Unique handle an app registers under (e.g. "nightly_report"). Lets
    -- `register_recurring` be idempotent/safe-on-every-boot, the same way
    -- `queue.migrate()` is: registering the same name again updates the
    -- definition in place instead of creating a duplicate schedule.
    name TEXT NOT NULL UNIQUE,
    queue TEXT NOT NULL DEFAULT 'default',
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    max_attempts INT NOT NULL DEFAULT 5,
    -- Standard 5-field cron syntax (minute hour day-of-month month
    -- day-of-week), parsed by `crate::cron::CronSchedule`.
    cron_expr TEXT NOT NULL,
    next_run_at TIMESTAMPTZ NOT NULL,
    -- Points at the most recent row this definition produced in
    -- `pgqueue_jobs`, for dashboard/debugging visibility ("what did this
    -- schedule last fire?"). Deliberately not a foreign key: nothing in
    -- this crate deletes rows from `pgqueue_jobs`, so there's nothing to
    -- cascade, and skipping the constraint keeps ad-hoc test/dev cleanup
    -- (`DELETE FROM pgqueue_jobs WHERE ...`) from needing to know about
    -- this table too.
    last_enqueued_job_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The scheduler's whole query is "which definitions are due" — this makes
-- that a range scan instead of a sequential one.
CREATE INDEX IF NOT EXISTS pgqueue_recurring_jobs_due_idx ON pgqueue_recurring_jobs (next_run_at);
