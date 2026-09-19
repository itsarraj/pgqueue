use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::Value;

use crate::queue::Queue;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type Handler = Arc<dyn Fn(Value) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Maps a job's `job_type` string to the async function that handles it.
/// This is how an app embeds pgqueue: register one closure per job kind,
/// then hand the registry to `run_worker`.
#[derive(Clone, Default)]
pub struct HandlerRegistry {
    handlers: HashMap<String, Handler>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F, Fut>(&mut self, job_type: impl Into<String>, handler: F) -> &mut Self
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.handlers.insert(
            job_type.into(),
            Arc::new(move |payload| Box::pin(handler(payload))),
        );
        self
    }

    pub fn get(&self, job_type: &str) -> Option<&Handler> {
        self.handlers.get(job_type)
    }
}

#[derive(Debug, Clone)]
pub struct WorkerOptions {
    pub poll_interval: Duration,
    pub backoff_base: Duration,
    pub backoff_cap: Duration,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            backoff_base: Duration::from_secs(2),
            backoff_cap: Duration::from_secs(300),
        }
    }
}

/// Runs forever: claim a due job, dispatch it to the registered handler for
/// its `job_type`, mark it done or failed. A job whose `job_type` has no
/// registered handler is treated as a failure (with a descriptive error) so
/// it goes through the normal retry/dead-lettering path rather than being
/// silently dropped.
///
/// While idle, this also `LISTEN`s on the `pgqueue_<queue_name>` channel
/// that the migration's insert trigger `NOTIFY`s (see
/// `migrations/0001_init.sql`) so a freshly-enqueued job is usually picked
/// up within milliseconds instead of waiting out `poll_interval`. This is a
/// latency optimization layered on top of the poll loop, never a
/// replacement for it: a `NOTIFY` sent while nobody is listening — the
/// worker hasn't started yet, or its listener connection is mid-reconnect —
/// is simply lost (Postgres does not queue notifications), and a job whose
/// `run_at` is in the future when inserted needs the poller to notice once
/// it actually comes due, since no second `NOTIFY` fires for it. So if the
/// `LISTEN` connection can't be established, or ever errors out, this logs
/// a warning and falls back to pure polling for the rest of the run rather
/// than failing the worker.
///
/// Call this from its own `tokio::spawn`'d task (or its own process) — it
/// does not return until an unrecoverable DB error occurs.
pub async fn run_worker(
    queue: &Queue,
    queue_name: &str,
    worker_id: &str,
    registry: &HandlerRegistry,
    opts: &WorkerOptions,
) -> Result<()> {
    let channel = format!("pgqueue_{queue_name}");
    let mut listener = match sqlx::postgres::PgListener::connect_with(queue.pool()).await {
        Ok(mut l) => match l.listen(&channel).await {
            Ok(()) => Some(l),
            Err(e) => {
                log::warn!(
                    "worker {worker_id}: failed to LISTEN on '{channel}' ({e}) — \
                     falling back to poll-only (poll_interval={:?})",
                    opts.poll_interval
                );
                None
            }
        },
        Err(e) => {
            log::warn!(
                "worker {worker_id}: could not open a LISTEN connection ({e}) — \
                 falling back to poll-only (poll_interval={:?})",
                opts.poll_interval
            );
            None
        }
    };

    loop {
        match queue.claim_next(queue_name, worker_id).await? {
            Some(job) => {
                log::info!(
                    "worker {worker_id}: claimed job {} (type={}, queue={})",
                    job.id,
                    job.job_type,
                    job.queue
                );
                let outcome = match registry.get(&job.job_type) {
                    Some(handler) => handler(job.payload.clone()).await,
                    None => Err(anyhow::anyhow!(
                        "no handler registered for job_type '{}'",
                        job.job_type
                    )),
                };
                match outcome {
                    Ok(()) => {
                        queue.complete(job.id).await?;
                        log::info!("worker {worker_id}: job {} done", job.id);
                    }
                    Err(e) => {
                        log::warn!("worker {worker_id}: job {} failed: {e}", job.id);
                        queue
                            .fail(job.id, &e.to_string(), opts.backoff_base, opts.backoff_cap)
                            .await?;
                    }
                }
            }
            // Nothing due right now: sleep out the poll interval, but wake
            // early if a NOTIFY arrives on our channel in the meantime.
            None => match listener.as_mut() {
                Some(l) => {
                    tokio::select! {
                        res = l.recv() => {
                            if let Err(e) = res {
                                log::warn!(
                                    "worker {worker_id}: LISTEN connection error ({e}) — \
                                     falling back to poll-only for the rest of this run"
                                );
                                listener = None;
                            }
                            // Ok(_): woken early by NOTIFY — loop back and claim now.
                        }
                        _ = tokio::time::sleep(opts.poll_interval) => {}
                    }
                }
                None => tokio::time::sleep(opts.poll_interval).await,
            },
        }
    }
}
