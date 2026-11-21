//! Rust twin of the Go probe worker: same kinds, effects and settings.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use riverqueue::jiff::Timestamp;
use riverqueue::sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use riverqueue::sqlx::{self, PgPool};
use riverqueue::{
    BoxError, Client, IntervalSchedule, Job, JobArgs, JobRow, MaintenanceConfig, PeriodicJob, PeriodicJobOpts,
    QueueConfig, RetryPolicy, WorkCancelled, WorkContext, WorkError, WorkOutcome, Workers,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "ledger")]
struct LedgerArgs { seq: i64 }

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "flaky")]
struct FlakyArgs { seq: i64, fail_first: i32 }

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "snooze")]
struct SnoozeArgs { seq: i64, snoozes: i64 }

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "cancel")]
struct CancelArgs { seq: i64 }

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "pause")]
struct PauseArgs { seq: i64 }

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "uniq")]
struct UniqArgs { key: i64, seq: i64 }

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "tick", unique(by_period = "2s"))]
struct TickArgs {}

struct Env { pool: PgPool, id: String, seed: AtomicU64 }

impl Env {
    fn rand(&self, n: u64) -> u64 {
        // xorshift: enough for sleeps and coin flips
        let mut x = self.seed.load(Ordering::Relaxed);
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        self.seed.store(x, Ordering::Relaxed);
        x % n
    }

    // Best effort, like the Go worker: a missing row can hide a violation but never invent one.
    async fn record(&self, row: &JobRow) {
        for _ in 0..3 {
            let r = sqlx::query("INSERT INTO probe_attempts (job_id, kind, attempt, max_attempts, attempted_at, scheduled_at, worker) VALUES ($1, $2, $3, $4, $5::timestamptz, $6::timestamptz, $7)")
                .bind(row.id).bind(&row.kind).bind(row.attempt).bind(row.max_attempts)
                .bind(row.attempted_at.map(|t| t.to_string())).bind(row.scheduled_at.to_string()).bind(&self.id)
                .execute(&self.pool).await;
            if r.is_ok() { return; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn complete(&self, context: &WorkContext, row: &JobRow, seq: i64) -> Result<(), BoxError> {
        let mut tx = riverqueue::database::begin_postgres(&self.pool).await?;
        sqlx::query("INSERT INTO probe_effects (job_id, kind, seq, attempt, worker) VALUES ($1, $2, $3, $4, $5)")
            .bind(row.id).bind(&row.kind).bind(seq).bind(row.attempt).bind(&self.id)
            .execute(&mut *tx).await?;
        context.job_complete_tx(&mut tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

struct ProbeRetry;

impl RetryPolicy for ProbeRetry {
    // 8 s for uniq: longer than the scheduler interval, so they wait in retryable.
    fn next_retry(&self, job: &JobRow, _error: &WorkError, _now: Timestamp) -> Duration {
        if job.kind == "uniq" { Duration::from_secs(8) } else { Duration::from_secs(1) }
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let id = std::env::args().nth(1).expect("worker id");
    let opts: PgConnectOptions = std::env::var("PROBE_DSN")?.parse()?;
    let pool = PgPoolOptions::new().max_connections(40)
        .connect_with(opts.application_name(&format!("probe-{id}"))).await?;
    let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos() as u64 | 1;
    let env = Arc::new(Env { pool: pool.clone(), id: id.clone(), seed: AtomicU64::new(seed) });

    let mut workers = Workers::new();
    let e = env.clone();
    workers.add_fn(move |ctx: WorkContext, job: Job<LedgerArgs>| { let e = e.clone(); async move {
        e.record(&job.row).await;
        tokio::time::sleep(Duration::from_millis(e.rand(300))).await;
        e.complete(&ctx, &job.row, job.args.seq).await?;
        Ok::<_, BoxError>(WorkOutcome::Complete)
    }})?;
    let e = env.clone();
    workers.add_fn(move |ctx: WorkContext, job: Job<FlakyArgs>| { let e = e.clone(); async move {
        e.record(&job.row).await;
        if job.row.attempt <= job.args.fail_first {
            return Err::<WorkOutcome, BoxError>(format!("planned failure {}", job.row.attempt).into());
        }
        e.complete(&ctx, &job.row, job.args.seq).await?;
        Ok(WorkOutcome::Complete)
    }})?;
    let e = env.clone();
    workers.add_fn(move |ctx: WorkContext, job: Job<SnoozeArgs>| { let e = e.clone(); async move {
        e.record(&job.row).await;
        let (done,): (i64,) = sqlx::query_as("SELECT count(*) FROM probe_snoozes WHERE job_id = $1")
            .bind(job.row.id).fetch_one(&e.pool).await?;
        if done < job.args.snoozes {
            sqlx::query("INSERT INTO probe_snoozes (job_id) VALUES ($1)").bind(job.row.id).execute(&e.pool).await?;
            return Ok::<_, BoxError>(WorkOutcome::Snooze(Duration::from_millis(200 + e.rand(800))));
        }
        e.complete(&ctx, &job.row, job.args.seq).await?;
        Ok(WorkOutcome::Complete)
    }})?;
    let e = env.clone();
    workers.add_fn(move |ctx: WorkContext, job: Job<CancelArgs>| { let e = e.clone(); async move {
        e.record(&job.row).await;
        let wait = Duration::from_millis(e.rand(1200));
        tokio::select! {
            () = ctx.cancellation_token().cancelled() => return Err::<WorkOutcome, BoxError>(WorkCancelled.into()),
            () = tokio::time::sleep(wait) => {}
        }
        match e.rand(3) {
            0 => Ok(WorkOutcome::Snooze(Duration::from_millis(300))),
            1 => Err("planned failure".into()),
            _ => { e.complete(&ctx, &job.row, job.args.seq).await?; Ok(WorkOutcome::Complete) }
        }
    }})?;
    let e = env.clone();
    workers.add_fn(move |ctx: WorkContext, job: Job<PauseArgs>| { let e = e.clone(); async move {
        e.record(&job.row).await;
        e.complete(&ctx, &job.row, job.args.seq).await?;
        Ok::<_, BoxError>(WorkOutcome::Complete)
    }})?;
    let e = env.clone();
    workers.add_fn(move |ctx: WorkContext, job: Job<UniqArgs>| { let e = e.clone(); async move {
        e.record(&job.row).await;
        if job.row.attempt == 1 && job.row.id % 2 == 0 {
            return Err::<WorkOutcome, BoxError>("planned first-attempt failure".into());
        }
        e.complete(&ctx, &job.row, job.args.seq).await?;
        Ok(WorkOutcome::Complete)
    }})?;
    let e = env.clone();
    workers.add_fn(move |ctx: WorkContext, job: Job<TickArgs>| { let e = e.clone(); async move {
        e.record(&job.row).await;
        let mut tx = riverqueue::database::begin_postgres(&e.pool).await?;
        sqlx::query("INSERT INTO probe_ticks (job_id, scheduled_at, worker) VALUES ($1, $2::timestamptz, $3)")
            .bind(job.row.id).bind(job.row.scheduled_at.to_string()).bind(&e.id)
            .execute(&mut *tx).await?;
        ctx.job_complete_tx(&mut tx).await?;
        tx.commit().await?;
        Ok::<_, BoxError>(WorkOutcome::Complete)
    }})?;

    let poll = Duration::from_millis(100);
    let client = Client::builder(pool)
        .id(id)
        .workers(workers)
        .queue("default", QueueConfig::new(20).with_fetch_poll_interval(poll))
        .queue("pausable", QueueConfig::new(5).with_fetch_poll_interval(poll))
        .job_timeout(Duration::from_secs(2))
        .maintenance(MaintenanceConfig::default().with_rescue_after(Duration::from_secs(4)))
        .retry_policy(ProbeRetry)
        .periodic_job(PeriodicJob::with_options(
            IntervalSchedule::new(Duration::from_secs(2))?,
            || TickArgs {},
            PeriodicJobOpts::new().with_run_on_start(true),
        ))
        .build()?;
    let mut run = client.start()?;
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    term.recv().await;
    run.stop().await?;
    Ok(())
}
