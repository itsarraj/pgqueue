//! Integration test against a real Postgres. Requires `PGQUEUE_TEST_DATABASE_URL`
//! to be set to a connectable Postgres instance; skips (with a message, not a
//! failure) if it isn't, so `cargo test` still passes in an environment with
//! no database configured. See README.md for how to stand one up locally.

use std::time::Duration;

use pgqueue::scheduler::run_scheduler;
use pgqueue::worker::{run_worker, HandlerRegistry, WorkerOptions};
use pgqueue::Queue;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

/// `cargo test` runs these concurrently by default, all against the same
/// shared Postgres — so cleanup is scoped to `queue_name` (each test uses
/// its own distinct queue name) rather than a blanket `DELETE FROM
/// pgqueue_jobs`, which raced across tests and intermittently wiped rows a
/// different, simultaneously-running test had just inserted.
async fn test_queue(queue_name: &str) -> Option<Queue> {
    let url = std::env::var("PGQUEUE_TEST_DATABASE_URL").ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("connect to test database");
    let queue = Queue::new(pool);
    queue.migrate().await.expect("run migrations");
    sqlx::query("DELETE FROM pgqueue_jobs WHERE queue = $1")
        .bind(queue_name)
        .execute(queue.pool())
        .await
        .expect("clear this test's own queue");
    Some(queue)
}

#[tokio::test]
async fn enqueue_claim_complete_round_trip() {
    let Some(queue) = test_queue("default").await else {
        eprintln!("skipping: PGQUEUE_TEST_DATABASE_URL not set");
        return;
    };

    let id = queue
        .enqueue("default", "greet", json!({"name": "world"}), 3)
        .await
        .unwrap();

    // Nothing claims from a different queue name.
    assert!(queue
        .claim_next("other-queue", "w1")
        .await
        .unwrap()
        .is_none());

    let claimed = queue.claim_next("default", "w1").await.unwrap().unwrap();
    assert_eq!(claimed.id, id);
    assert_eq!(claimed.job_type, "greet");
    assert_eq!(claimed.payload, json!({"name": "world"}));
    assert_eq!(claimed.status, "running");
    assert_eq!(claimed.attempts, 1);

    // Claimed already — a second worker gets nothing.
    assert!(queue.claim_next("default", "w2").await.unwrap().is_none());

    queue.complete(id).await.unwrap();

    let stats = queue.stats(Some("default")).await.unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].done, 1);
    assert_eq!(stats[0].queued, 0);
    assert_eq!(stats[0].running, 0);
}

#[tokio::test]
async fn two_concurrent_workers_never_claim_the_same_job() {
    let Some(queue) = test_queue("concurrency").await else {
        eprintln!("skipping: PGQUEUE_TEST_DATABASE_URL not set");
        return;
    };

    for i in 0..10 {
        queue
            .enqueue("concurrency", "noop", json!({"i": i}), 3)
            .await
            .unwrap();
    }

    let q1 = queue.clone();
    let q2 = queue.clone();
    let (a, b) = tokio::join!(
        async move {
            let mut ids = Vec::new();
            while let Some(j) = q1.claim_next("concurrency", "w1").await.unwrap() {
                ids.push(j.id);
            }
            ids
        },
        async move {
            let mut ids = Vec::new();
            while let Some(j) = q2.claim_next("concurrency", "w2").await.unwrap() {
                ids.push(j.id);
            }
            ids
        }
    );

    let mut all: Vec<i64> = a.into_iter().chain(b.into_iter()).collect();
    all.sort_unstable();
    all.dedup();
    assert_eq!(
        all.len(),
        10,
        "every job claimed exactly once across both workers"
    );
}

#[tokio::test]
async fn failed_job_retries_then_goes_dead_after_max_attempts() {
    let Some(queue) = test_queue("retries").await else {
        eprintln!("skipping: PGQUEUE_TEST_DATABASE_URL not set");
        return;
    };

    let id = queue
        .enqueue("retries", "flaky", json!({}), 2)
        .await
        .unwrap();

    // Attempt 1: claim, fail. attempts=1 < max_attempts=2 -> requeued.
    let job = queue.claim_next("retries", "w1").await.unwrap().unwrap();
    assert_eq!(job.attempts, 1);
    queue
        .fail(id, "boom", Duration::from_millis(0), Duration::from_secs(1))
        .await
        .unwrap();

    let stats = queue.stats(Some("retries")).await.unwrap();
    assert_eq!(stats[0].queued, 1, "requeued for another attempt");
    assert_eq!(stats[0].dead, 0);

    // Attempt 2: claim, fail again. attempts=2 >= max_attempts=2 -> dead.
    let job = queue.claim_next("retries", "w1").await.unwrap().unwrap();
    assert_eq!(job.attempts, 2);
    queue
        .fail(
            id,
            "boom again",
            Duration::from_millis(0),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

    let stats = queue.stats(Some("retries")).await.unwrap();
    assert_eq!(stats[0].dead, 1);
    assert_eq!(stats[0].queued, 0);

    // Dead jobs are not claimable...
    assert!(queue.claim_next("retries", "w1").await.unwrap().is_none());

    // ...until explicitly requeued.
    assert!(queue.requeue_dead(id).await.unwrap());
    let job = queue.claim_next("retries", "w1").await.unwrap().unwrap();
    assert_eq!(job.id, id);
    assert_eq!(
        job.attempts, 1,
        "requeue_dead resets attempts to 0 before this claim increments it"
    );
}

#[tokio::test]
async fn run_at_in_the_future_is_not_claimable_yet() {
    let Some(queue) = test_queue("scheduled").await else {
        eprintln!("skipping: PGQUEUE_TEST_DATABASE_URL not set");
        return;
    };

    queue
        .enqueue_at(
            "scheduled",
            "later",
            json!({}),
            1,
            chrono::Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .unwrap();

    assert!(queue.claim_next("scheduled", "w1").await.unwrap().is_none());

    let stats = queue.stats(Some("scheduled")).await.unwrap();
    assert_eq!(stats[0].queued, 1, "still queued, just not due yet");
}

/// Proves `run_worker`'s `LISTEN`/`NOTIFY` wake-up is real, not just wired
/// up and untested: runs a worker with a deliberately huge 10s
/// `poll_interval`, enqueues one job, and asserts it's claimed and
/// completed in well under that 10s — the only way that's possible is the
/// insert trigger's `NOTIFY` waking the worker's `PgListener` early, since
/// the poll loop alone would sleep out the full 10s first.
#[tokio::test]
async fn listen_notify_wakes_worker_before_poll_interval_elapses() {
    let Some(queue) = test_queue("listen").await else {
        eprintln!("skipping: PGQUEUE_TEST_DATABASE_URL not set");
        return;
    };

    let mut registry = HandlerRegistry::new();
    registry.register("noop", |_payload| async move { Ok(()) });

    let worker_queue = queue.clone();
    let opts = WorkerOptions {
        poll_interval: Duration::from_secs(10),
        ..Default::default()
    };
    let worker = tokio::spawn(async move {
        let _ = run_worker(&worker_queue, "listen", "listen-test-worker", &registry, &opts).await;
    });

    // Give the worker a moment to establish its LISTEN connection before we
    // enqueue. If we enqueued first, we'd be racing NOTIFY against LISTEN
    // startup — a real race (a NOTIFY sent before anyone is listening is
    // simply lost), and exactly why the poll loop has to stay as a
    // fallback rather than this test proving LISTEN alone is sufficient.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let start = tokio::time::Instant::now();
    let id = queue.enqueue("listen", "noop", json!({}), 1).await.unwrap();

    let deadline = start + Duration::from_secs(5);
    let mut done_at = None;
    while tokio::time::Instant::now() < deadline {
        let jobs = queue.recent_jobs(Some("listen"), 5).await.unwrap();
        if jobs.iter().any(|j| j.id == id && j.status == "done") {
            done_at = Some(tokio::time::Instant::now());
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    worker.abort();

    let done_at = done_at.expect(
        "job should have completed well before the 10s poll_interval if NOTIFY wake-up works",
    );
    let elapsed = done_at - start;
    assert!(
        elapsed < Duration::from_secs(2),
        "expected a NOTIFY-driven wake-up in well under the 10s poll_interval, took {elapsed:?}"
    );
}

/// Proves a recurring job definition actually re-fires on its schedule via
/// the real `run_scheduler` loop (not just a single `tick_recurring()`
/// call): forces the definition due, lets the running scheduler pick it up
/// and materialize a job, forces it due a *second* time, and confirms a
/// *different* job gets enqueued the second time too — the "recurring"
/// part, not a one-shot.
#[tokio::test]
async fn recurring_job_fires_repeatedly_via_running_scheduler() {
    let Some(queue) = test_queue("recurring").await else {
        eprintln!("skipping: PGQUEUE_TEST_DATABASE_URL not set");
        return;
    };

    // This test's own recurring definition, cleaned up by name (mirrors how
    // `test_queue` scopes job cleanup by queue name) so repeated runs don't
    // collide with a leftover row from a previous run.
    sqlx::query("DELETE FROM pgqueue_recurring_jobs WHERE name = $1")
        .bind("recurring_test")
        .execute(queue.pool())
        .await
        .expect("clear this test's own recurring definition");

    queue
        .register_recurring(
            "recurring_test",
            "recurring",
            "log",
            json!({"n": 1}),
            3,
            "* * * * *",
        )
        .await
        .unwrap();

    // register_recurring computes next_run_at as "the next whole minute" -
    // up to 60s away. Force it due right now rather than making this test
    // wait on a real minute boundary.
    sqlx::query("UPDATE pgqueue_recurring_jobs SET next_run_at = now() WHERE name = $1")
        .bind("recurring_test")
        .execute(queue.pool())
        .await
        .unwrap();

    // Run the actual production entry point in the background, same as an
    // app embedding this crate would.
    let scheduler_queue = queue.clone();
    let scheduler =
        tokio::spawn(
            async move { let _ = run_scheduler(&scheduler_queue, Duration::from_millis(100)).await; },
        );

    let first_job_id = wait_for_job(&queue, "recurring", None, Duration::from_secs(5))
        .await
        .expect("recurring job should have fired once within 5s");

    // Force it due again — this is the actual "recurring" assertion: the
    // same definition firing a second, independent time on its own
    // schedule, not just the first occurrence.
    sqlx::query("UPDATE pgqueue_recurring_jobs SET next_run_at = now() WHERE name = $1")
        .bind("recurring_test")
        .execute(queue.pool())
        .await
        .unwrap();

    let second_job_id = wait_for_job(
        &queue,
        "recurring",
        Some(first_job_id),
        Duration::from_secs(5),
    )
    .await
    .expect("recurring job should have fired a second time within 5s");

    scheduler.abort();

    assert_ne!(
        first_job_id, second_job_id,
        "each occurrence must be its own distinct pgqueue_jobs row"
    );

    let stats = queue.stats(Some("recurring")).await.unwrap();
    assert_eq!(stats[0].queued, 2, "both occurrences materialized as real jobs");

    let def = queue
        .get_recurring("recurring_test")
        .await
        .unwrap()
        .expect("definition still exists");
    assert_eq!(
        def.last_enqueued_job_id,
        Some(second_job_id),
        "bookkeeping points at the most recently enqueued occurrence"
    );
    assert!(
        def.next_run_at > chrono::Utc::now(),
        "next_run_at was advanced into the future again, not left stuck in the past"
    );
}

/// Polls `recent_jobs` until a job appears on `queue_name` other than
/// `exclude_id`, or the deadline passes.
async fn wait_for_job(
    queue: &Queue,
    queue_name: &str,
    exclude_id: Option<i64>,
    timeout: Duration,
) -> Option<i64> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let jobs = queue.recent_jobs(Some(queue_name), 10).await.unwrap();
        if let Some(j) = jobs.iter().find(|j| Some(j.id) != exclude_id) {
            return Some(j.id);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}
