use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::backoff::backoff_delay;
use crate::cron::CronSchedule;
use crate::job::{Job, QueueStats, RecurringJob};

/// The main handle applications embed: `Queue::new(pool)`, then
/// `queue.enqueue(...)` from a request handler, and a separate worker task
/// calls `queue.claim_next(...)` in a loop (see `worker::run_worker`).
#[derive(Clone)]
pub struct Queue {
    pool: PgPool,
}

impl Queue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Runs the bundled migrations. Safe to call on every app boot — every
    /// statement is `CREATE ... IF NOT EXISTS` / `CREATE OR REPLACE`.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub async fn enqueue(
        &self,
        queue: &str,
        job_type: &str,
        payload: serde_json::Value,
        max_attempts: i32,
    ) -> Result<i64> {
        self.enqueue_at(queue, job_type, payload, max_attempts, Utc::now())
            .await
    }

    pub async fn enqueue_at(
        &self,
        queue: &str,
        job_type: &str,
        payload: serde_json::Value,
        max_attempts: i32,
        run_at: DateTime<Utc>,
    ) -> Result<i64> {
        let (id,): (i64,) = sqlx::query_as(
            r#"
            INSERT INTO pgqueue_jobs (queue, job_type, payload, max_attempts, run_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
        )
        .bind(queue)
        .bind(job_type)
        .bind(payload)
        .bind(max_attempts)
        .bind(run_at)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    /// Atomically claims the oldest due job on `queue` for `worker_id`, or
    /// `None` if nothing is due. `SKIP LOCKED` is what makes this safe with
    /// any number of concurrent workers polling the same table — a worker
    /// never blocks on a row another worker already grabbed, it just moves
    /// on to the next one.
    pub async fn claim_next(&self, queue: &str, worker_id: &str) -> Result<Option<Job>> {
        let job = sqlx::query_as::<_, Job>(
            r#"
            UPDATE pgqueue_jobs
            SET status = 'running', attempts = attempts + 1, locked_at = now(), locked_by = $1
            WHERE id = (
                SELECT id FROM pgqueue_jobs
                WHERE queue = $2 AND status = 'queued' AND run_at <= now()
                ORDER BY run_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
            "#,
        )
        .bind(worker_id)
        .bind(queue)
        .fetch_optional(&self.pool)
        .await?;
        Ok(job)
    }

    pub async fn complete(&self, job_id: i64) -> Result<()> {
        sqlx::query("UPDATE pgqueue_jobs SET status = 'done', completed_at = now() WHERE id = $1")
            .bind(job_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Records a failed attempt. Requeues with exponential backoff if
    /// attempts remain, otherwise marks the job `dead` (parked, not retried
    /// automatically — see `requeue_dead`).
    pub async fn fail(
        &self,
        job_id: i64,
        error: &str,
        backoff_base: Duration,
        backoff_cap: Duration,
    ) -> Result<()> {
        let (attempts, max_attempts): (i32, i32) =
            sqlx::query_as("SELECT attempts, max_attempts FROM pgqueue_jobs WHERE id = $1")
                .bind(job_id)
                .fetch_one(&self.pool)
                .await?;

        if attempts >= max_attempts {
            sqlx::query(
                "UPDATE pgqueue_jobs SET status = 'dead', last_error = $2, locked_at = NULL, locked_by = NULL WHERE id = $1",
            )
            .bind(job_id)
            .bind(error)
            .execute(&self.pool)
            .await?;
        } else {
            let delay = backoff_delay(attempts as u32, backoff_base, backoff_cap);
            let run_at =
                Utc::now() + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::zero());
            sqlx::query(
                "UPDATE pgqueue_jobs SET status = 'queued', run_at = $2, last_error = $3, locked_at = NULL, locked_by = NULL WHERE id = $1",
            )
            .bind(job_id)
            .bind(run_at)
            .bind(error)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    /// Puts a `dead` job back in the queue for a fresh set of attempts.
    /// This is the dashboard's "retry" button and the CLI's equivalent.
    pub async fn requeue_dead(&self, job_id: i64) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE pgqueue_jobs SET status = 'queued', attempts = 0, run_at = now(), last_error = NULL WHERE id = $1 AND status = 'dead'",
        )
        .bind(job_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn stats(&self, queue: Option<&str>) -> Result<Vec<QueueStats>> {
        let rows = sqlx::query_as::<_, QueueStats>(
            r#"
            SELECT
                queue,
                count(*) FILTER (WHERE status = 'queued') AS queued,
                count(*) FILTER (WHERE status = 'running') AS running,
                count(*) FILTER (WHERE status = 'done') AS done,
                count(*) FILTER (WHERE status = 'failed') AS failed,
                count(*) FILTER (WHERE status = 'dead') AS dead
            FROM pgqueue_jobs
            WHERE $1::text IS NULL OR queue = $1
            GROUP BY queue
            ORDER BY queue
            "#,
        )
        .bind(queue)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn recent_jobs(&self, queue: Option<&str>, limit: i64) -> Result<Vec<Job>> {
        let rows = sqlx::query_as::<_, Job>(
            r#"
            SELECT * FROM pgqueue_jobs
            WHERE $1::text IS NULL OR queue = $1
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(queue)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Registers (or updates) a recurring job definition: from now on,
    /// `name` re-enqueues `job_type`/`payload` onto `queue` every time
    /// `cron_expr` says it's due, via `tick_recurring`
    /// (`scheduler::run_scheduler` drives that on a timer).
    ///
    /// Idempotent and safe to call on every app boot, the same way
    /// `migrate()` is: calling it again with the same `name` updates the
    /// definition in place instead of registering a duplicate schedule. If
    /// `cron_expr` is unchanged from what's already stored, the existing
    /// `next_run_at` is preserved rather than recomputed — so a routine app
    /// restart doesn't silently push a job's next fire time later. If
    /// `cron_expr` did change, `next_run_at` is recomputed from now using
    /// the new expression.
    pub async fn register_recurring(
        &self,
        name: &str,
        queue: &str,
        job_type: &str,
        payload: serde_json::Value,
        max_attempts: i32,
        cron_expr: &str,
    ) -> Result<i64> {
        let schedule = CronSchedule::parse(cron_expr)?;
        let next_run_at = schedule.next_after(Utc::now())?;

        let (id,): (i64,) = sqlx::query_as(
            r#"
            INSERT INTO pgqueue_recurring_jobs (name, queue, job_type, payload, max_attempts, cron_expr, next_run_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (name) DO UPDATE SET
                queue = EXCLUDED.queue,
                job_type = EXCLUDED.job_type,
                payload = EXCLUDED.payload,
                max_attempts = EXCLUDED.max_attempts,
                cron_expr = EXCLUDED.cron_expr,
                next_run_at = CASE
                    WHEN pgqueue_recurring_jobs.cron_expr = EXCLUDED.cron_expr
                        THEN pgqueue_recurring_jobs.next_run_at
                    ELSE EXCLUDED.next_run_at
                END
            RETURNING id
            "#,
        )
        .bind(name)
        .bind(queue)
        .bind(job_type)
        .bind(payload)
        .bind(max_attempts)
        .bind(cron_expr)
        .bind(next_run_at)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn list_recurring(&self) -> Result<Vec<RecurringJob>> {
        let rows =
            sqlx::query_as::<_, RecurringJob>("SELECT * FROM pgqueue_recurring_jobs ORDER BY name")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows)
    }

    pub async fn get_recurring(&self, name: &str) -> Result<Option<RecurringJob>> {
        let row =
            sqlx::query_as::<_, RecurringJob>("SELECT * FROM pgqueue_recurring_jobs WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    /// Enqueues a concrete `pgqueue_jobs` row for every recurring
    /// definition whose `next_run_at` is due, advancing each to its next
    /// occurrence in the same transaction it was claimed in. Returns the
    /// ids of the jobs it enqueued (mainly for logging/tests).
    ///
    /// Safe to call from any number of concurrent scheduler processes:
    /// `FOR UPDATE SKIP LOCKED` means only one of them enqueues + advances
    /// any given definition per tick, and the others simply skip it — the
    /// exact same idiom `claim_next` uses for jobs, applied here to
    /// recurring-job rows instead of job rows. No leader election needed.
    pub async fn tick_recurring(&self) -> Result<Vec<i64>> {
        let mut tx = self.pool.begin().await?;
        let due = sqlx::query_as::<_, RecurringJob>(
            "SELECT * FROM pgqueue_recurring_jobs WHERE next_run_at <= now() FOR UPDATE SKIP LOCKED",
        )
        .fetch_all(&mut *tx)
        .await?;

        let mut enqueued = Vec::with_capacity(due.len());
        for r in due {
            // Re-parses `cron_expr` rather than caching a `CronSchedule` —
            // ticks are infrequent (seconds, not per-job) so this cost is
            // negligible, and it means an app can never end up running a
            // stale in-memory schedule after editing a row directly in the
            // database.
            let schedule = match CronSchedule::parse(&r.cron_expr) {
                Ok(s) => s,
                Err(e) => {
                    log::error!(
                        "recurring job '{}': cron_expr '{}' no longer parses ({e}) — skipping this tick",
                        r.name,
                        r.cron_expr
                    );
                    continue;
                }
            };
            let now = Utc::now();
            let next_run_at = match schedule.next_after(now) {
                Ok(t) => t,
                Err(e) => {
                    log::error!(
                        "recurring job '{}': could not compute next run ({e}) — skipping this tick",
                        r.name
                    );
                    continue;
                }
            };

            let (job_id,): (i64,) = sqlx::query_as(
                r#"
                INSERT INTO pgqueue_jobs (queue, job_type, payload, max_attempts, run_at)
                VALUES ($1, $2, $3, $4, now())
                RETURNING id
                "#,
            )
            .bind(&r.queue)
            .bind(&r.job_type)
            .bind(&r.payload)
            .bind(r.max_attempts)
            .fetch_one(&mut *tx)
            .await?;

            sqlx::query(
                "UPDATE pgqueue_recurring_jobs SET next_run_at = $2, last_enqueued_job_id = $3 WHERE id = $1",
            )
            .bind(r.id)
            .bind(next_run_at)
            .bind(job_id)
            .execute(&mut *tx)
            .await?;

            enqueued.push(job_id);
        }

        tx.commit().await?;
        Ok(enqueued)
    }
}
