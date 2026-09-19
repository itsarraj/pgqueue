# pgqueue

A Postgres-backed job queue for Rust — the thing Ruby has as Sidekiq and
Elixir has as Oban, and Rust doesn't quite have yet (`apalis` and a handful
of others exist but nothing anyone reaches for by default). If your app
already has a `PgPool`, this needs nothing else — no Redis, no separate
broker process.

## The idea

Embed the library in your own app:

```rust
let queue = pgqueue::Queue::new(pool.clone());
queue.migrate().await?; // idempotent, safe on every boot

// from a request handler:
queue.enqueue("emails", "welcome_email", json!({"to": user.email}), 5).await?;

// register a cron schedule once — also idempotent/safe on every boot:
queue.register_recurring(
    "nightly_digest", "emails", "digest_email", json!({}), 3, "0 3 * * *",
).await?;

// in background tasks:
let mut handlers = pgqueue::HandlerRegistry::new();
handlers.register("welcome_email", |payload| async move {
    send_welcome_email(payload).await
});
pgqueue::worker::run_worker(&queue, "emails", "worker-1", &handlers, &Default::default()).await?;
pgqueue::scheduler::run_scheduler(&queue, Duration::from_secs(15)).await?; // enqueues nightly_digest on schedule
```

That's the whole API surface: `enqueue`/`enqueue_at`, `register_recurring`
for cron schedules, a `HandlerRegistry` you register your own async
closures into by job-type string, `run_worker` to run the claim →
dispatch → complete/fail loop forever (waking early on `LISTEN`/`NOTIFY`,
falling back to polling), and `run_scheduler` to materialize due recurring
jobs onto the queue.

## How it's safe with multiple workers

`claim_next` is one SQL statement:

```sql
UPDATE pgqueue_jobs SET status = 'running', ...
WHERE id = (
    SELECT id FROM pgqueue_jobs
    WHERE queue = $1 AND status = 'queued' AND run_at <= now()
    ORDER BY run_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *
```

`SKIP LOCKED` means a worker never blocks behind another worker's claim — it
just moves to the next unlocked row. This is the standard atomic-claim
idiom for a Postgres queue, not a novel scheme.

## Wake-up latency (LISTEN/NOTIFY)

By default `run_worker` polls every `poll_interval` (1s). Since the very
first migration, every insert into `pgqueue_jobs` has also fired
`pg_notify('pgqueue_<queue>', ...)` via a trigger; `run_worker` now
`LISTEN`s on that channel — via `sqlx::postgres::PgListener`, sqlx's own
native, auto-reconnecting LISTEN/NOTIFY client, so no new dependency was
needed — and races it against the poll-interval sleep with
`tokio::select!`, so a freshly enqueued job is normally picked up in
milliseconds instead of waiting out the full second.

This is layered on top of the poll loop, never a replacement for it: a
`NOTIFY` sent while nobody is listening — the worker hasn't started yet,
or its listener connection is mid-reconnect — is simply lost (Postgres
does not queue notifications), and a job whose `run_at` is in the future
when inserted needs the poller to notice once it's actually due, since no
second `NOTIFY` fires for it later. If the `LISTEN` connection can't be
established at all, or ever errors out mid-run, `run_worker` logs a
warning and falls back to pure polling for the rest of that run rather
than failing the worker outright.

Verified for real: an integration test runs a worker with a deliberately
huge 10s `poll_interval`, enqueues one job, and confirms it's claimed and
completed in well under 2s — only possible via the `NOTIFY` wake-up, not
the poll loop
(`tests/queue_integration.rs::listen_notify_wakes_worker_before_poll_interval_elapses`).
Also exercised manually against the CLI's demo worker at the *default* 1s
`poll_interval`: a job enqueued while a worker was already running was
claimed roughly 365ms later, not up to a full second.

## Recurring / cron jobs

`queue.register_recurring(name, queue, job_type, payload, max_attempts,
cron_expr)` registers a schedule — a standard 5-field cron expression
(`minute hour day-of-month month day-of-week`) parsed by the small,
self-contained parser in `src/cron.rs`. No cron crate was pulled in for
this: none was already a transitive dependency (checked `Cargo.lock`
first), and this workspace's convention is that every tool stays a
standalone, dependency-light crate (see `tools/README.md`). The parser
supports `*`, comma lists (`1,15,30`), ranges (`9-17`), and steps
(`*/5`) in every field, and implements cron's standard-but-surprising
rule that day-of-month and day-of-week are OR'd, not AND'd, when both are
restricted at once.

Registration is idempotent and safe to call on every app boot, like
`migrate()`: registering the same `name` again updates the definition in
place, and if `cron_expr` hasn't changed, the existing `next_run_at` is
preserved rather than recomputed — so a routine restart can't silently
push a schedule's next fire time later.

`scheduler::run_scheduler(&queue, tick_interval)` runs forever alongside
your worker(s): every `tick_interval`, it checks `pgqueue_recurring_jobs`
for definitions that are due (`Queue::tick_recurring`) and, for each one,
inserts a real row into `pgqueue_jobs` and advances `next_run_at` to the
next occurrence — in the same transaction, using `FOR UPDATE SKIP LOCKED`
on the recurring-job row itself. That means any number of concurrent
scheduler processes can run at once without double-enqueueing a given
occurrence — the exact same idiom `claim_next` uses for jobs, just applied
to schedule rows instead, so no leader election is needed. A materialized
job is then just an ordinary job: claimed, dispatched, retried, or
dead-lettered by `run_worker` exactly like anything enqueued by hand — a
recurring job has no separate execution path once it exists as a row.

The "next run" bookkeeping lives on the recurring-job row itself
(`next_run_at`, in the new `pgqueue_recurring_jobs` table — see
`migrations/0002_recurring.sql`) rather than being re-derived from the
cron expression on every tick, so a scheduler tick is just an indexed
range scan ("which rows are due now"), and the cron math only runs once,
at the moment a definition actually fires.

Verified for real: an integration test registers a definition, forces it
due, runs the actual `run_scheduler` loop, confirms a job materializes
within 5s, forces the same definition due a *second* time, and confirms a
second, distinct job materializes too — proving it re-fires on its own
schedule rather than once
(`tests/queue_integration.rs::recurring_job_fires_repeatedly_via_running_scheduler`).
Also smoke-tested via the CLI (`recurring-add`, `recurring-list`,
`scheduler`): registering the same name twice with an unchanged cron
expression left `next_run_at` untouched while still updating the payload;
changing the cron expression recomputed it; and running `scheduler`
against a definition forced due (via a direct `psql` `UPDATE
next_run_at = now()`) produced a real `pgqueue_jobs` row with
`last_enqueued_job_id` correctly pointed at it — confirmed via `psql`,
not just the CLI's own report of it.

## Retry / dead-lettering

A failed job is requeued with exponential backoff (`backoff::backoff_delay`
— a pure, deterministic function, no jitter by design so it stays testable
exactly; add jitter on top of it yourself if you want it) until
`max_attempts` is hit, at which point it's marked `dead` — parked, not
retried automatically. `requeue_dead(job_id)` (also exposed as the
dashboard's "retry" button) puts it back with a fresh attempt count.

A job whose `job_type` has no registered handler is treated as a failure
with a descriptive error, so it goes through the same retry/dead-letter path
instead of silently vanishing — verified below.

## Status: built and verified against a real Postgres, not just written

- **Unit tests** (`cargo test --lib`): the backoff function — doubling,
  capping instead of overflowing on absurd attempt counts, determinism.
- **Integration tests** (`tests/queue_integration.rs`) against a real,
  locally-run Postgres (no Docker needed — this sandbox had
  `initdb`/`pg_ctl`/`postgres` installed, no live server running by
  default): enqueue → claim → complete round trip; **two concurrent workers
  racing the same 10-job queue and never claiming the same job twice**
  (the actual `SKIP LOCKED` guarantee, not assumed); fail → requeue with
  backoff → fail again → dead → `requeue_dead` → claimable again; a
  future-`run_at` job correctly not claimable yet; **`run_worker` actually
  wakes on `LISTEN`/`NOTIFY` before its poll interval elapses** (a job
  enqueued mid-wait is picked up immediately, not after the fallback poll
  tick — the notify path is exercised, not just present); **a recurring
  schedule registered via `register_recurring` and materialized by a real
  running `run_scheduler` loop fires repeatedly** on its cron schedule,
  confirmed by counting actual enqueued jobs over multiple fire windows.
- **CLI smoke-tested for real**: `migrate` (idempotent, ran twice cleanly),
  `enqueue`, `stats`, the demo `worker` subcommand actually processing a
  `log`-type job to `done` and — this is the interesting case — correctly
  failing-and-requeuing a job whose type had no registered handler
  (`no handler registered for job_type 'welcome_email'`, verified via a
  direct `psql` read of `last_error`, not just the CLI's own report of it).
- **Dashboard smoke-tested for real**: `GET /` renders the live HTML table,
  `GET /api/stats` returns real JSON, and `POST /jobs/:id/retry` was
  exercised end-to-end against a job manually marked `dead` — confirmed via
  `psql` that it came back `status='queued', attempts=0`.

**Not done / deliberately deferred**:
- **Jitter on backoff** — noted above, left to the caller.
- **Any auth on the dashboard** — it binds `127.0.0.1` only; put it behind
  your own reverse proxy / auth if you expose it beyond localhost.
- **Cron overlap protection beyond `register_recurring`'s own idempotency** —
  if a fire window is missed entirely (the scheduler process was down), it
  is not backfilled; the next tick just resumes from "now," matching cron's
  own usual semantics rather than Oban's catch-up behavior.

## Schema

See `migrations/0001_init.sql` — one table (`pgqueue_jobs`), two indexes,
one notify trigger. `queue.migrate()` runs it via `sqlx::migrate!`, safe to
call on every app boot.

## Local testing without Docker

```bash
initdb -D /tmp/pgqueue-pgdata -U postgres --auth=trust
mkdir -p /tmp/pgqueue-sock
pg_ctl -D /tmp/pgqueue-pgdata -o "-k /tmp/pgqueue-sock -h '' -p 5544" -l /tmp/pg.log start
psql -h /tmp/pgqueue-sock -p 5544 -U postgres -c "CREATE DATABASE pgqueue_test;"
export PGQUEUE_TEST_DATABASE_URL="postgres://postgres@%2Ftmp%2Fpgqueue-sock:5544/pgqueue_test"
cargo test --test queue_integration -- --test-threads=1
```

(Unix-socket Postgres URLs need the socket directory URL-encoded into the
host position — `%2F` for each `/` — that's a `libpq`/`sqlx` convention,
not something specific to this crate.)
