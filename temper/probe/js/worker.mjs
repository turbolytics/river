// JS twin of the Go probe worker: same kinds, same effects, same settings.
import { createRequire } from "node:module";
import { Client, Workers, defineJob, periodicJob, snooze } from "/app/dist/index.js";
import { PgDriver } from "/app/driver/pg/dist/index.js";

const pg = createRequire("/app/driver/pg/package.json")("pg");
const id = process.argv[2];
const pool = new pg.Pool({ connectionString: process.env.PROBE_DSN, max: 40, application_name: "probe-" + id });
pool.on("error", () => {});

const def = (kind) => defineJob()({ kind });
const ledger = def("ledger"), flaky = def("flaky"), snoozeJob = def("snooze"), cancelJob = def("cancel");
const pauseJob = def("pause"), uniq = def("uniq"), tick = def("tick");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rand = (n) => Math.floor(Math.random() * n);

function sleepSignal(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason ?? new Error("aborted"));
    const t = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => { clearTimeout(t); reject(signal.reason ?? new Error("aborted")); }, { once: true });
  });
}

// node-postgres emits connection errors on a checked-out client; without a
// listener they crash the process.
async function inTx(fn) {
  const tx = await pool.connect();
  let broken;
  const onError = (e) => { broken = e; };
  tx.on("error", onError);
  try {
    await tx.query("BEGIN");
    await fn(tx);
    await tx.query("COMMIT");
  } catch (e) {
    broken ??= e;
    await tx.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    if (broken) {
      tx.once("end", () => tx.off("error", onError)); // a destroyed client can emit once more
    } else {
      tx.off("error", onError);
    }
    tx.release(broken);
  }
}

// Best effort, like the Go worker: a missing row can hide a violation but never invent one.
async function record(job) {
  for (let i = 0; i < 3; i++) {
    try {
      await pool.query(
        "INSERT INTO probe_attempts (job_id, kind, attempt, max_attempts, attempted_at, scheduled_at, worker) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        [job.id.toString(), job.kind, job.attempt, job.maxAttempts, job.attemptedAt?.toString() ?? null, job.scheduledAt.toString(), id]);
      return;
    } catch { await sleep(100); }
  }
}

async function complete(job, completeTx, seq) {
  await inTx(async (tx) => {
    await tx.query("INSERT INTO probe_effects (job_id, kind, seq, attempt, worker) VALUES ($1, $2, $3, $4, $5)",
      [job.id.toString(), job.kind, seq, job.attempt, id]);
    await completeTx(tx);
  });
}

const workers = new Workers()
  .add(ledger, async ({ completeTx, job }) => {
    await record(job);
    await sleep(rand(300));
    await complete(job, completeTx, job.args.seq);
  })
  .add(flaky, async ({ completeTx, job }) => {
    await record(job);
    if (job.attempt <= job.args.fail_first) throw new Error(`planned failure ${job.attempt}`);
    await complete(job, completeTx, job.args.seq);
  })
  .add(snoozeJob, async ({ completeTx, job }) => {
    await record(job);
    const { rows } = await pool.query("SELECT count(*)::int AS n FROM probe_snoozes WHERE job_id = $1", [job.id.toString()]);
    if (rows[0].n < job.args.snoozes) {
      await pool.query("INSERT INTO probe_snoozes (job_id) VALUES ($1)", [job.id.toString()]);
      return snooze({ milliseconds: 200 + rand(800) });
    }
    await complete(job, completeTx, job.args.seq);
  })
  .add(cancelJob, async ({ completeTx, job, signal }) => {
    await record(job);
    await sleepSignal(rand(1200), signal);
    const r = rand(3);
    if (r === 0) return snooze({ milliseconds: 300 });
    if (r === 1) throw new Error("planned failure");
    await complete(job, completeTx, job.args.seq);
  })
  .add(pauseJob, async ({ completeTx, job }) => {
    await record(job);
    await complete(job, completeTx, job.args.seq);
  })
  .add(uniq, async ({ completeTx, job }) => {
    await record(job);
    if (job.attempt === 1 && job.id % 2n === 0n) throw new Error("planned first-attempt failure");
    await complete(job, completeTx, job.args.seq);
  })
  .add(tick, async ({ completeTx, job }) => {
    await record(job);
    await inTx(async (tx) => {
      await tx.query("INSERT INTO probe_ticks (job_id, scheduled_at, worker) VALUES ($1, $2, $3)",
        [job.id.toString(), job.scheduledAt.toString(), id]);
      await completeTx(tx);
    });
  });

const client = new Client(new PgDriver(pool), {
  clientId: id,
  queues: {
    default: { maxWorkers: 20, pollInterval: { milliseconds: 100 } },
    pausable: { maxWorkers: 5, pollInterval: { milliseconds: 100 } },
  },
  workers,
  jobTimeout: { seconds: 2 },
  maintenance: { rescueAfter: { seconds: 4 } },
  // 8 s for uniq: longer than the scheduler interval, so they wait in retryable.
  retryPolicy: (job, now) => now.add({ seconds: job.kind === "uniq" ? 8 : 1 }),
  logger: false,
  periodicJobs: [
    periodicJob({
      job: tick,
      args: {},
      every: { seconds: 2 },
      options: { unique: { byPeriod: { seconds: 2 } } },
      runOnStart: true,
    }),
  ],
});

const run = await client.start();
process.once("SIGTERM", () => void run.stop({ timeout: { seconds: 5 } }));
await run.completed.catch((e) => { console.error(id, "fatal", e); process.exit(1); });
await pool.end().catch(() => {});
