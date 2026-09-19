//! Drives recurring/cron jobs — this is pgqueue's answer to Oban's Cron
//! plugin. Register a definition once with `Queue::register_recurring`
//! (name, queue, job_type, payload, cron expression), run `run_scheduler`
//! in the background, and a concrete job materializes into the ordinary
//! `pgqueue_jobs` table on schedule (`Queue::tick_recurring`), advancing
//! the definition to its next occurrence in the same transaction. From
//! there it's just a normal job: claimed, dispatched, retried, or
//! dead-lettered by `worker::run_worker` exactly like anything enqueued by
//! hand — recurring jobs don't have a separate execution path.

use std::time::Duration;

use anyhow::Result;

use crate::queue::Queue;

/// Runs forever: every `tick_interval`, enqueues whatever recurring job
/// definitions are due (see `Queue::tick_recurring`) and sleeps.
///
/// `tick_interval` should be shorter than the finest-grained cron
/// expression you register — cron's own granularity is one minute, so
/// something in the 10-30s range is reasonable; this is a coarse poll, not
/// backed by LISTEN/NOTIFY, since nothing generates a notification for
/// "time has passed."
///
/// Safe to run from multiple processes/replicas at once — see
/// `Queue::tick_recurring` for why concurrent scheduler instances don't
/// double-enqueue the same occurrence.
///
/// Call this from its own `tokio::spawn`'d task (or its own process), same
/// as `worker::run_worker` — it does not return until an unrecoverable DB
/// error occurs.
pub async fn run_scheduler(queue: &Queue, tick_interval: Duration) -> Result<()> {
    loop {
        match queue.tick_recurring().await {
            Ok(ids) if !ids.is_empty() => {
                log::info!(
                    "scheduler: enqueued {} recurring job(s): {:?}",
                    ids.len(),
                    ids
                );
            }
            Ok(_) => {}
            Err(e) => {
                log::error!("scheduler: tick_recurring failed: {e}");
            }
        }
        tokio::time::sleep(tick_interval).await;
    }
}
