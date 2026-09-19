CREATE TABLE IF NOT EXISTS pgqueue_jobs (
    id BIGSERIAL PRIMARY KEY,
    queue TEXT NOT NULL DEFAULT 'default',
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'queued',
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT pgqueue_jobs_status_check CHECK (status IN ('queued', 'running', 'done', 'failed', 'dead'))
);

CREATE INDEX IF NOT EXISTS pgqueue_jobs_poll_idx ON pgqueue_jobs (queue, status, run_at);
CREATE INDEX IF NOT EXISTS pgqueue_jobs_status_idx ON pgqueue_jobs (status);

-- Notify a channel per queue on insert, so a worker can LISTEN instead of
-- pure polling. Workers still need a fallback poll loop (a NOTIFY sent while
-- nobody is listening is simply lost — Postgres does not queue it), so this
-- is a latency optimization on top of polling, not a replacement for it.
CREATE OR REPLACE FUNCTION pgqueue_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('pgqueue_' || NEW.queue, NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS pgqueue_notify_trigger ON pgqueue_jobs;
CREATE TRIGGER pgqueue_notify_trigger
    AFTER INSERT ON pgqueue_jobs
    FOR EACH ROW
    EXECUTE FUNCTION pgqueue_notify();
