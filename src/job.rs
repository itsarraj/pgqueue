use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Queued,
    Running,
    Done,
    Failed,
    /// Exhausted all retries — parked for manual inspection/requeue, never
    /// picked up by a worker again on its own.
    Dead,
}

impl JobStatus {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Running => "running",
            JobStatus::Done => "done",
            JobStatus::Failed => "failed",
            JobStatus::Dead => "dead",
        }
    }

    pub fn from_db_str(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(JobStatus::Queued),
            "running" => Some(JobStatus::Running),
            "done" => Some(JobStatus::Done),
            "failed" => Some(JobStatus::Failed),
            "dead" => Some(JobStatus::Dead),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Job {
    pub id: i64,
    pub queue: String,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub status: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub run_at: DateTime<Utc>,
    pub locked_at: Option<DateTime<Utc>>,
    pub locked_by: Option<String>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, FromRow)]
pub struct QueueStats {
    pub queue: String,
    pub queued: i64,
    pub running: i64,
    pub done: i64,
    pub failed: i64,
    pub dead: i64,
}

/// A recurring/cron job definition (`pgqueue_recurring_jobs`) — a template
/// that `Queue::tick_recurring` periodically materializes into an ordinary
/// `Job` row once `next_run_at` is due. See `crate::scheduler`.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct RecurringJob {
    pub id: i64,
    pub name: String,
    pub queue: String,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub max_attempts: i32,
    pub cron_expr: String,
    pub next_run_at: DateTime<Utc>,
    pub last_enqueued_job_id: Option<i64>,
    pub created_at: DateTime<Utc>,
}
