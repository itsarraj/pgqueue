mod dashboard;

use std::time::Duration;

use clap::{Parser, Subcommand};
use pgqueue::scheduler::run_scheduler;
use pgqueue::worker::{run_worker, HandlerRegistry, WorkerOptions};
use pgqueue::Queue;
use sqlx::postgres::PgPoolOptions;

#[derive(Parser)]
#[command(
    name = "pgqueue",
    about = "A Postgres-backed job queue — Sidekiq/Oban without Redis"
)]
struct Cli {
    /// Postgres connection string. Falls back to $DATABASE_URL.
    #[arg(long, global = true)]
    database_url: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the bundled migrations (idempotent, safe on every boot).
    Migrate,
    /// Enqueue a single job from the command line.
    Enqueue {
        #[arg(long, default_value = "default")]
        queue: String,
        #[arg(long)]
        job_type: String,
        /// JSON payload, e.g. '{"to":"a@b.com"}'.
        #[arg(long, default_value = "{}")]
        payload: String,
        #[arg(long, default_value_t = 5)]
        max_attempts: i32,
    },
    /// Print per-queue counts by status.
    Stats {
        #[arg(long)]
        queue: Option<String>,
    },
    /// Put a dead job back in the queue.
    Retry { job_id: i64 },
    /// Run a demo worker with a couple of built-in example handlers
    /// (`log`, `sleep_ms`, `fail`). Real usage is embedding the `pgqueue`
    /// library directly in your own app with your own handlers registered
    /// — this subcommand exists to exercise and demonstrate the queue
    /// mechanics standalone, not as the primary way to consume this crate.
    Worker {
        #[arg(long, default_value = "default")]
        queue: String,
        #[arg(long, default_value = "cli-worker")]
        worker_id: String,
    },
    /// Serve the tiny HTML + JSON dashboard.
    Dashboard {
        #[arg(long, default_value_t = 8787)]
        port: u16,
    },
    /// Register (or update) a recurring/cron job definition. Safe to run
    /// again with the same --name — it updates the definition in place.
    RecurringAdd {
        /// Unique name for this schedule, e.g. "nightly_report".
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "default")]
        queue: String,
        #[arg(long)]
        job_type: String,
        #[arg(long, default_value = "{}")]
        payload: String,
        #[arg(long, default_value_t = 5)]
        max_attempts: i32,
        /// 5-field cron expression, e.g. "*/15 * * * *" or "0 3 * * *".
        #[arg(long)]
        cron: String,
    },
    /// List registered recurring/cron job definitions.
    RecurringList,
    /// Run the recurring-job scheduler loop: enqueues due recurring jobs
    /// (registered via `recurring-add`) on schedule. Run alongside one or
    /// more `worker` processes — the scheduler only enqueues, it doesn't
    /// execute jobs itself.
    Scheduler {
        #[arg(long, default_value_t = 15)]
        tick_secs: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let database_url = cli
        .database_url
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .ok_or_else(|| anyhow::anyhow!("no --database-url given and $DATABASE_URL is not set"))?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    let queue = Queue::new(pool);

    match cli.command {
        Command::Migrate => {
            queue.migrate().await?;
            println!("migrations applied");
        }
        Command::Enqueue {
            queue: q,
            job_type,
            payload,
            max_attempts,
        } => {
            let payload: serde_json::Value = serde_json::from_str(&payload)?;
            let id = queue.enqueue(&q, &job_type, payload, max_attempts).await?;
            println!("enqueued job {id} on queue '{q}'");
        }
        Command::Stats { queue: q } => {
            let stats = queue.stats(q.as_deref()).await?;
            println!(
                "{:<15} {:>8} {:>8} {:>8} {:>8} {:>8}",
                "queue", "queued", "running", "done", "failed", "dead"
            );
            for s in stats {
                println!(
                    "{:<15} {:>8} {:>8} {:>8} {:>8} {:>8}",
                    s.queue, s.queued, s.running, s.done, s.failed, s.dead
                );
            }
        }
        Command::Retry { job_id } => {
            if queue.requeue_dead(job_id).await? {
                println!("job {job_id} requeued");
            } else {
                println!("job {job_id} not found or not dead");
            }
        }
        Command::Worker {
            queue: q,
            worker_id,
        } => {
            let mut registry = HandlerRegistry::new();
            registry.register("log", |payload| async move {
                log::info!("[demo:log] {payload}");
                Ok(())
            });
            registry.register("sleep_ms", |payload| async move {
                let ms = payload.get("ms").and_then(|v| v.as_u64()).unwrap_or(1000);
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            });
            registry.register("fail", |payload| async move {
                anyhow::bail!("demo failure: {payload}")
            });
            println!("worker '{worker_id}' running on queue '{q}' (demo handlers: log, sleep_ms, fail) — Ctrl-C to stop");
            run_worker(&queue, &q, &worker_id, &registry, &WorkerOptions::default()).await?;
        }
        Command::Dashboard { port } => {
            dashboard::run(queue, port).await?;
        }
        Command::RecurringAdd {
            name,
            queue: q,
            job_type,
            payload,
            max_attempts,
            cron,
        } => {
            let payload: serde_json::Value = serde_json::from_str(&payload)?;
            let id = queue
                .register_recurring(&name, &q, &job_type, payload, max_attempts, &cron)
                .await?;
            println!("recurring job '{name}' registered (id={id}, cron='{cron}')");
        }
        Command::RecurringList => {
            let defs = queue.list_recurring().await?;
            println!(
                "{:<20} {:<12} {:<15} {:<18} {:<28}",
                "name", "queue", "job_type", "cron", "next_run_at"
            );
            for d in defs {
                println!(
                    "{:<20} {:<12} {:<15} {:<18} {:<28}",
                    d.name, d.queue, d.job_type, d.cron_expr, d.next_run_at
                );
            }
        }
        Command::Scheduler { tick_secs } => {
            println!(
                "scheduler running (tick_interval={tick_secs}s) — Ctrl-C to stop"
            );
            run_scheduler(&queue, Duration::from_secs(tick_secs)).await?;
        }
    }

    Ok(())
}
