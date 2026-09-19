//! `pgqueue` — a Postgres-backed job queue. The intended usage is as a
//! library embedded in your own Actix/Axum/whatever app: hold a `Queue` next
//! to your `PgPool`, call `enqueue` from request handlers, and run
//! `worker::run_worker` in a background task with your own handlers
//! registered. See `README.md` for the pitch and `src/main.rs` for a CLI
//! that exercises the same library for migration, one-off enqueue, stats,
//! and a small HTTP dashboard.

pub mod backoff;
pub mod cron;
pub mod job;
pub mod queue;
pub mod scheduler;
pub mod worker;

pub use cron::CronSchedule;
pub use job::{Job, JobStatus, QueueStats, RecurringJob};
pub use queue::Queue;
pub use scheduler::run_scheduler;
pub use worker::{HandlerRegistry, WorkerOptions};
