// Probe for River's guarantees under faults.
//
//	riverprobe run -workers 3 -js 0 -duration 120s -faults kill,stop,pgrestart,pgcrash,pgpause,terminate
//	riverprobe worker -id w1   (spawned by run; ids starting with "j" are JS containers)
//
// Job kinds, each with the docs' promise it tests:
//
//	ledger   effect + JobCompleteTx; a fifth are scheduled in the future
//	flaky    fails its first N attempts, max_attempts 1-4
//	snooze   snoozes 4 times with max_attempts 2, then completes
//	cancel   long attempts that snooze, error or complete; the controller cancels half
//	pause    in queue "pausable", which the controller pauses and resumes
//	uniq     unique by args without retryable; half fail their first attempt
//	tick     periodic, unique per 2 s period, run on start
//
// Every attempt is logged (probe_attempts) with River's own attempted_at and
// scheduled_at, so timing invariants are checked in the database's clock.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

var dsn = envOr("PROBE_DSN", "postgres://postgres:temper@localhost:26532/river?sslmode=disable")

const (
	pgContainer = "temper-river-pg"
	tickPeriod  = 2 * time.Second
	// A client that misses the pause notification (its LISTEN connection was
	// down) catches up on a fallback poll every minute (riverqueue/river#1582).
	pauseGrace = 65 * time.Second
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// --- jobs -------------------------------------------------------------------

type LedgerArgs struct {
	Seq int64 `json:"seq"`
}

func (LedgerArgs) Kind() string { return "ledger" }

type FlakyArgs struct {
	Seq       int64 `json:"seq"`
	FailFirst int   `json:"fail_first"`
}

func (FlakyArgs) Kind() string { return "flaky" }

type SnoozeArgs struct {
	Seq     int64 `json:"seq"`
	Snoozes int   `json:"snoozes"`
}

func (SnoozeArgs) Kind() string { return "snooze" }

type CancelArgs struct {
	Seq int64 `json:"seq"`
}

func (CancelArgs) Kind() string { return "cancel" }

type PauseArgs struct {
	Seq int64 `json:"seq"`
}

func (PauseArgs) Kind() string { return "pause" }

type UniqArgs struct {
	Key int   `json:"key" river:"unique"`
	Seq int64 `json:"seq"`
}

func (UniqArgs) Kind() string { return "uniq" }

var uniqOpts = river.UniqueOpts{ByArgs: true, ByState: []rivertype.JobState{
	rivertype.JobStateAvailable, rivertype.JobStateCompleted, rivertype.JobStatePending,
	rivertype.JobStateRunning, rivertype.JobStateScheduled,
}}

type TickArgs struct{}

func (TickArgs) Kind() string { return "tick" }

type env struct {
	pool *pgxpool.Pool
	id   string
}

// record logs the attempt with River's timestamps. Best effort: a missing row
// can hide a violation but never invent one.
func (e *env) record(row *rivertype.JobRow) {
	var at time.Time
	if row.AttemptedAt != nil {
		at = *row.AttemptedAt
	}
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := e.pool.Exec(ctx, `INSERT INTO probe_attempts (job_id, kind, attempt, max_attempts, attempted_at, scheduled_at, worker)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, row.ID, row.Kind, row.Attempt, row.MaxAttempts, at, row.ScheduledAt, e.id)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// complete commits the effect and the completion in one transaction.
func complete[T river.JobArgs](ctx context.Context, e *env, job *river.Job[T], seq int64) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `INSERT INTO probe_effects (job_id, kind, seq, attempt, worker) VALUES ($1, $2, $3, $4, $5)`,
		job.ID, job.Kind, seq, job.Attempt, e.id); err != nil {
		return err
	}
	if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type LedgerWorker struct {
	river.WorkerDefaults[LedgerArgs]
	*env
}

func (w *LedgerWorker) Work(ctx context.Context, job *river.Job[LedgerArgs]) error {
	w.record(job.JobRow)
	time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)
	return complete(ctx, w.env, job, job.Args.Seq)
}

type FlakyWorker struct {
	river.WorkerDefaults[FlakyArgs]
	*env
}

func (w *FlakyWorker) Work(ctx context.Context, job *river.Job[FlakyArgs]) error {
	w.record(job.JobRow)
	if job.Attempt <= job.Args.FailFirst {
		return fmt.Errorf("planned failure %d", job.Attempt)
	}
	return complete(ctx, w.env, job, job.Args.Seq)
}

type SnoozeWorker struct {
	river.WorkerDefaults[SnoozeArgs]
	*env
}

func (w *SnoozeWorker) Work(ctx context.Context, job *river.Job[SnoozeArgs]) error {
	w.record(job.JobRow)
	var done int
	if err := w.pool.QueryRow(ctx, `SELECT count(*) FROM probe_snoozes WHERE job_id = $1`, job.ID).Scan(&done); err != nil {
		return err
	}
	if done < job.Args.Snoozes {
		if _, err := w.pool.Exec(ctx, `INSERT INTO probe_snoozes (job_id) VALUES ($1)`, job.ID); err != nil {
			return err
		}
		return river.JobSnooze(time.Duration(200+rand.Intn(800)) * time.Millisecond)
	}
	return complete(ctx, w.env, job, job.Args.Seq)
}

type CancelWorker struct {
	river.WorkerDefaults[CancelArgs]
	*env
}

func (w *CancelWorker) Work(ctx context.Context, job *river.Job[CancelArgs]) error {
	w.record(job.JobRow)
	if err := sleepCtx(ctx, time.Duration(rand.Intn(1200))*time.Millisecond); err != nil {
		return err
	}
	switch rand.Intn(3) {
	case 0:
		return river.JobSnooze(300 * time.Millisecond)
	case 1:
		return errors.New("planned failure")
	}
	return complete(ctx, w.env, job, job.Args.Seq)
}

type PauseWorker struct {
	river.WorkerDefaults[PauseArgs]
	*env
}

func (w *PauseWorker) Work(ctx context.Context, job *river.Job[PauseArgs]) error {
	w.record(job.JobRow)
	return complete(ctx, w.env, job, job.Args.Seq)
}

type UniqWorker struct {
	river.WorkerDefaults[UniqArgs]
	*env
}

func (w *UniqWorker) Work(ctx context.Context, job *river.Job[UniqArgs]) error {
	w.record(job.JobRow)
	if job.Attempt == 1 && job.ID%2 == 0 {
		return errors.New("planned first-attempt failure")
	}
	return complete(ctx, w.env, job, job.Args.Seq)
}

type TickWorker struct {
	river.WorkerDefaults[TickArgs]
	*env
}

func (w *TickWorker) Work(ctx context.Context, job *river.Job[TickArgs]) error {
	w.record(job.JobRow)
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `INSERT INTO probe_ticks (job_id, scheduled_at, worker) VALUES ($1, $2, $3)`,
		job.ID, job.ScheduledAt, w.id); err != nil {
		return err
	}
	if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type fixedRetry struct{}

// NextRetry retries after 1 s, or after 8 s for uniq jobs: longer than the
// scheduler's 5 s interval, so they wait in retryable, outside the unique states.
func (fixedRetry) NextRetry(row *rivertype.JobRow) time.Time {
	if row.Kind == "uniq" {
		return time.Now().Add(8 * time.Second)
	}
	return time.Now().Add(time.Second)
}

// --- worker process ------------------------------------------------------------

func worker(id string) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	cfg, err := pgxpool.ParseConfig(dsn)
	must(err)
	cfg.ConnConfig.RuntimeParams["application_name"] = "probe-" + id
	cfg.MaxConns = 40
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	must(err)
	e := &env{pool: pool, id: id}
	workers := river.NewWorkers()
	river.AddWorker(workers, &LedgerWorker{env: e})
	river.AddWorker(workers, &FlakyWorker{env: e})
	river.AddWorker(workers, &SnoozeWorker{env: e})
	river.AddWorker(workers, &CancelWorker{env: e})
	river.AddWorker(workers, &PauseWorker{env: e})
	river.AddWorker(workers, &UniqWorker{env: e})
	river.AddWorker(workers, &TickWorker{env: e})
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		ID: id,
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 20},
			"pausable":         {MaxWorkers: 5},
		},
		Workers:              workers,
		JobTimeout:           2 * time.Second,
		RescueStuckJobsAfter: 4 * time.Second,
		FetchPollInterval:    100 * time.Millisecond,
		MaxAttempts:          1000,
		RetryPolicy:          fixedRetry{},
		PeriodicJobs: []*river.PeriodicJob{river.NewPeriodicJob(
			river.PeriodicInterval(tickPeriod),
			func() (river.JobArgs, *river.InsertOpts) {
				return TickArgs{}, &river.InsertOpts{UniqueOpts: river.UniqueOpts{ByPeriod: tickPeriod}}
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		)},
	})
	must(err)
	if err := client.Start(ctx); err != nil {
		log.Fatalf("%s start: %v", id, err)
	}
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = client.Stop(sctx)
}

// --- orchestrator ---------------------------------------------------------------

type cluster struct {
	mu    sync.Mutex
	procs map[string]*exec.Cmd
	bin   string
	logf  *os.File
}

// Ids starting with "j" are JS workers and "r" Ruby workers, each in a container.
// "v" ids are Java workers and "u" ids Rust workers.
func isContainer(id string) bool {
	return strings.HasPrefix(id, "j") || strings.HasPrefix(id, "r") || strings.HasPrefix(id, "v") || strings.HasPrefix(id, "u")
}

var probeDir, _ = os.Getwd()

func (c *cluster) spawn(id string) {
	if isContainer(id) {
		name := "probe-" + id
		exec.Command("docker", "rm", "-f", name).Run()
		dsnEnv := "PROBE_DSN=postgres://postgres:temper@host.docker.internal:26532/river"
		if strings.HasPrefix(id, "u") {
			sh("docker", "run", "-d", "--name", name, "--memory", "400m", "-v", "river-rust:/app",
				"-e", dsnEnv, "rust:1", "/app/probe/target/release/riverprobe", id)
		} else if strings.HasPrefix(id, "v") {
			sh("docker", "run", "-d", "--name", name, "--memory", "600m", "-v", "river-java:/app", "-v", probeDir+"/java:/probe:ro",
				"-e", dsnEnv, "eclipse-temurin:21", "java", "--enable-native-access=ALL-UNNAMED",
				"-cp", "/app/river/target/river-0.4.1.jar:/app/deps/*", "/probe/Worker.java", id)
		} else if strings.HasPrefix(id, "r") {
			sh("docker", "run", "-d", "--name", name, "--memory", "400m", "-v", "river-ruby:/app", "-v", probeDir+"/ruby:/probe:ro",
				"-e", dsnEnv, "-e", "BUNDLE_GEMFILE=/app/probe-gems/Gemfile", "-e", "BUNDLE_PATH=/app/.gems",
				"ruby:3.4", "bundle", "exec", "ruby", "/probe/worker.rb", id)
		} else {
			sh("docker", "run", "-d", "--name", name, "--memory", "400m", "-v", "river-js:/app", "-v", probeDir+"/js:/probe:ro",
				"-e", dsnEnv, "node:26", "node", "/probe/worker.mjs", id)
		}
		return
	}
	cmd := exec.Command(c.bin, "worker", "-id", id)
	cmd.Stdout, cmd.Stderr = c.logf, c.logf
	must(cmd.Start())
	c.mu.Lock()
	c.procs[id] = cmd
	c.mu.Unlock()
	go cmd.Wait()
}

func (c *cluster) signal(id string, s syscall.Signal) {
	if isContainer(id) {
		name := "probe-" + id
		switch s {
		case syscall.SIGKILL:
			sh("docker", "kill", name)
		case syscall.SIGSTOP:
			sh("docker", "pause", name)
		case syscall.SIGCONT:
			sh("docker", "unpause", name)
		case syscall.SIGTERM:
			sh("docker", "stop", "-t", "5", name)
		}
		return
	}
	c.mu.Lock()
	p := c.procs[id]
	c.mu.Unlock()
	if p != nil {
		_ = p.Process.Signal(s)
	}
}

func sh(args ...string) {
	out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
	if err != nil {
		log.Printf("%v: %v %s", args, err, strings.TrimSpace(string(out)))
	}
}

func waitPG(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := pgx.Connect(context.Background(), dsn)
		if err == nil {
			conn.Close(context.Background())
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	log.Printf("postgres not back after %s", timeout)
}

// retry runs fn until it succeeds or the attempts run out.
func retry(n int, fn func() error) error {
	var err error
	for i := 0; i < n; i++ {
		if err = fn(); err == nil {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return err
}

func dbNow(pool *pgxpool.Pool) (time.Time, error) {
	var t time.Time
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&t)
	return t, err
}

func run(nWorkers, nJS, nRuby, nJava, nRust int, duration time.Duration, faults []string, seed int64) int {
	rng := rand.New(rand.NewSource(seed))
	var rngMu sync.Mutex
	rint := func(n int) int { rngMu.Lock(); defer rngMu.Unlock(); return rng.Intn(n) }
	ctx := context.Background()
	waitPG(60 * time.Second)
	pool, err := pgxpool.New(ctx, dsn)
	must(err)
	defer pool.Close()
	for _, q := range []string{
		`DROP SCHEMA public CASCADE`, `CREATE SCHEMA public`,
		`CREATE TABLE probe_inserted (seq bigint PRIMARY KEY, job_id bigint NOT NULL, kind text NOT NULL)`,
		`CREATE TABLE probe_rolledback (seq bigint PRIMARY KEY)`,
		`CREATE TABLE probe_effects (id bigserial, job_id bigint, kind text, seq bigint, attempt int, worker text, at timestamptz DEFAULT clock_timestamp())`,
		`CREATE TABLE probe_ticks (id bigserial, job_id bigint, scheduled_at timestamptz, worker text, at timestamptz DEFAULT clock_timestamp())`,
		`CREATE TABLE probe_attempts (job_id bigint, kind text, attempt int, max_attempts int, attempted_at timestamptz, scheduled_at timestamptz, worker text, at timestamptz DEFAULT clock_timestamp())`,
		`CREATE TABLE probe_snoozes (job_id bigint, at timestamptz DEFAULT clock_timestamp())`,
		`CREATE TABLE probe_cancels (job_id bigint PRIMARY KEY, returned_at timestamptz, state text)`,
		`CREATE TABLE probe_pauses (paused_at timestamptz, resuming_at timestamptz)`,
		`CREATE TABLE probe_faults (at timestamptz DEFAULT clock_timestamp(), fault text)`,
	} {
		_, err := pool.Exec(ctx, q)
		must(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	must(err)
	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	must(err)

	bin, _ := os.Executable()
	logf, _ := os.Create(fmt.Sprintf("workers-%d.log", seed))
	c := &cluster{procs: map[string]*exec.Cmd{}, bin: bin, logf: logf}
	ids := []string{}
	for i := 1; i <= nWorkers; i++ {
		ids = append(ids, fmt.Sprintf("w%d", i))
	}
	for i := 1; i <= nJS; i++ {
		ids = append(ids, fmt.Sprintf("j%d", i))
	}
	for i := 1; i <= nRuby; i++ {
		ids = append(ids, fmt.Sprintf("r%d", i))
	}
	for i := 1; i <= nJava; i++ {
		ids = append(ids, fmt.Sprintf("v%d", i))
	}
	for i := 1; i <= nRust; i++ {
		ids = append(ids, fmt.Sprintf("u%d", i))
	}
	for _, id := range ids {
		c.spawn(id)
	}

	ctl, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	must(err)
	var seq atomic.Int64
	var stopIns atomic.Bool
	var wg sync.WaitGroup
	var nCommitErr atomic.Int64

	// Inserters: transactional inserts of every kind; a quarter of ledger
	// inserts roll back.
	insertOne := func(s int64) {
		ictx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		var args river.JobArgs
		opts := &river.InsertOpts{}
		rollback := false
		switch r := rint(100); {
		case r < 40:
			args = LedgerArgs{Seq: s}
			rollback = s%4 == 0
			if r < 8 {
				opts.ScheduledAt = time.Now().Add(time.Duration(rint(4000)) * time.Millisecond)
			}
		case r < 55:
			args = FlakyArgs{Seq: s, FailFirst: rint(4)}
			opts.MaxAttempts = 1 + rint(4)
		case r < 65:
			args = SnoozeArgs{Seq: s, Snoozes: 4}
			opts.MaxAttempts = 2
		case r < 80:
			args = CancelArgs{Seq: s}
		case r < 90:
			args = PauseArgs{Seq: s}
			opts.Queue = "pausable"
		default:
			args = UniqArgs{Key: int(s/300)*10 + rint(10), Seq: s} // a fresh key set every ~300 inserts
			opts.UniqueOpts = uniqOpts
		}
		tx, err := pool.Begin(ictx)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			return
		}
		defer tx.Rollback(context.Background())
		res, err := ctl.InsertTx(ictx, tx, args, opts)
		if err != nil {
			return
		}
		if res.UniqueSkippedAsDuplicate {
			return
		}
		if _, err := tx.Exec(ictx, `INSERT INTO probe_inserted VALUES ($1, $2, $3)`, s, res.Job.ID, args.Kind()); err != nil {
			return
		}
		if rollback {
			if tx.Rollback(ictx) == nil {
				_ = retry(50, func() error {
					_, err := pool.Exec(context.Background(), `INSERT INTO probe_rolledback VALUES ($1)`, s)
					return err
				})
			}
			return
		}
		if err := tx.Commit(ictx); err != nil {
			nCommitErr.Add(1) // ambiguous: probe_inserted says whether it committed
		}
	}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stopIns.Load() {
				insertOne(seq.Add(1))
				time.Sleep(time.Duration(5+rint(10)) * time.Millisecond)
			}
		}()
	}

	// Controller: cancels half the cancel jobs and pauses the pausable queue.
	var stopCtl atomic.Bool
	var ctlWG sync.WaitGroup
	ctlWG.Add(2)
	go func() {
		defer ctlWG.Done()
		for !stopCtl.Load() {
			time.Sleep(time.Duration(100+rint(300)) * time.Millisecond)
			var id int64
			if pool.QueryRow(ctx, `SELECT i.job_id FROM probe_inserted i WHERE i.kind = 'cancel' AND i.seq % 2 = 0
				AND NOT EXISTS (SELECT 1 FROM probe_cancels c WHERE c.job_id = i.job_id) ORDER BY random() LIMIT 1`).Scan(&id) != nil {
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			row, err := ctl.JobCancel(cctx, id)
			cancel()
			if err != nil {
				continue
			}
			ret, err := dbNow(pool) // read after JobCancel returned: never earlier than the return
			if err != nil {
				continue
			}
			_ = retry(20, func() error {
				_, err := pool.Exec(ctx, `INSERT INTO probe_cancels VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, id, ret, string(row.State))
				return err
			})
		}
	}()
	go func() {
		defer ctlWG.Done()
		pauses := 0
		for !stopCtl.Load() {
			time.Sleep(time.Duration(3000+rint(3000)) * time.Millisecond)
			if retry(20, func() error { return ctl.QueuePause(ctx, "pausable", nil) }) != nil {
				continue
			}
			paused, err := dbNow(pool)
			// Every other pause outlasts the fallback poll, so a missed
			// notification must heal inside the window.
			pauses++
			hold := time.Duration(9000+rint(6000)) * time.Millisecond
			if pauses%2 == 1 {
				hold = 80 * time.Second
			}
			time.Sleep(hold)
			resuming, err2 := dbNow(pool) // read before resuming: never later than the resume
			if retry(100, func() error { return ctl.QueueResume(ctx, "pausable", nil) }) != nil {
				log.Printf("could not resume pausable")
			}
			if err == nil && err2 == nil {
				_ = retry(20, func() error {
					_, err := pool.Exec(ctx, `INSERT INTO probe_pauses VALUES ($1, $2)`, paused, resuming)
					return err
				})
			}
		}
	}()

	record := func(f string) {
		log.Printf("fault: %s", f)
		_ = retry(20, func() error {
			_, err := pool.Exec(ctx, `INSERT INTO probe_faults (fault) VALUES ($1)`, f)
			return err
		})
	}
	end := time.Now().Add(duration)
	time.Sleep(3 * time.Second)
	for len(faults) > 0 && time.Now().Before(end) {
		f := faults[rint(len(faults))]
		target := ids[rint(len(ids))]
		if rint(2) == 0 {
			var l string
			_ = pool.QueryRow(ctx, `SELECT leader_id FROM river_leader LIMIT 1`).Scan(&l)
			if l != "" {
				target = l
			}
		}
		switch f {
		case "kill":
			record("kill " + target)
			c.signal(target, syscall.SIGKILL)
			time.Sleep(time.Duration(500+rint(2000)) * time.Millisecond)
			c.spawn(target)
		case "stop":
			d := time.Duration(3000+rint(6000)) * time.Millisecond
			record(fmt.Sprintf("stop %s %s", target, d))
			c.signal(target, syscall.SIGSTOP)
			time.Sleep(d)
			c.signal(target, syscall.SIGCONT)
			record("cont " + target)
		case "pgrestart":
			record("pgrestart")
			sh("docker", "restart", "-t", "2", pgContainer)
			waitPG(60 * time.Second)
		case "pgcrash":
			record("pgcrash")
			sh("docker", "kill", "-s", "KILL", pgContainer)
			time.Sleep(time.Duration(rint(2000)) * time.Millisecond)
			sh("docker", "start", pgContainer)
			waitPG(60 * time.Second)
			record("pg back after crash")
		case "pgpause":
			d := time.Duration(2000+rint(6000)) * time.Millisecond
			record(fmt.Sprintf("pgpause %s", d))
			sh("docker", "pause", pgContainer)
			time.Sleep(d)
			sh("docker", "unpause", pgContainer)
		case "terminate":
			record("terminate backends")
			_, _ = pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name LIKE 'probe-%'`)
		}
		time.Sleep(time.Duration(2000+rint(4000)) * time.Millisecond)
	}
	for time.Now().Before(end) {
		time.Sleep(time.Second)
	}
	stopIns.Store(true)
	wg.Wait()
	stopCtl.Store(true)
	ctlWG.Wait()
	record("quiesce")

	// Drain: wait while jobs still finish. Fail only when progress stops for
	// 60 s (or after 10 minutes), so a backlog is not mistaken for a stall.
	hardStop := time.Now().Add(10 * time.Minute)
	var left int
	best, lastProgress := -1, time.Now()
	for time.Now().Before(hardStop) && time.Since(lastProgress) < 60*time.Second {
		left = -1
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE kind <> 'tick' AND state NOT IN ('completed','discarded','cancelled')`).Scan(&left)
		if left == 0 {
			break
		}
		if left >= 0 && (best < 0 || left < best) {
			best, lastProgress = left, time.Now()
		}
		time.Sleep(time.Second)
	}
	record(fmt.Sprintf("drained, %d left", left))
	for _, id := range ids {
		c.signal(id, syscall.SIGTERM)
	}
	time.Sleep(3 * time.Second)
	for _, id := range ids {
		if isContainer(id) {
			out, _ := exec.Command("docker", "logs", "probe-"+id).CombinedOutput()
			fmt.Fprintf(logf, "=== %s\n%s", id, out)
			exec.Command("docker", "rm", "-f", "probe-"+id).Run()
		} else {
			c.signal(id, syscall.SIGKILL)
		}
	}
	return check(ctx, pool, seed, left, nCommitErr.Load())
}

var checks = []struct{ name, sql string }{
	// At-least-once and transactional enqueue.
	{"lost", `SELECT count(*) FROM probe_inserted i JOIN river_job j ON j.id = i.job_id
		WHERE i.kind IN ('ledger','pause') AND NOT EXISTS (SELECT 1 FROM probe_effects e WHERE e.job_id = i.job_id)`},
	{"lost_row", `SELECT count(*) FROM probe_inserted i WHERE NOT EXISTS (SELECT 1 FROM river_job j WHERE j.id = i.job_id)`},
	{"phantom", `SELECT count(*) FROM probe_rolledback r WHERE EXISTS (SELECT 1 FROM probe_effects e WHERE e.seq = r.seq)
		OR EXISTS (SELECT 1 FROM probe_inserted i WHERE i.seq = r.seq)`},
	{"unfinished", `SELECT count(*) FROM river_job WHERE kind <> 'tick' AND state NOT IN ('completed','discarded','cancelled')`},
	// JobCompleteTx: an effect commits at most once, and only with completion.
	{"dup_effect", `SELECT count(*) FROM (SELECT job_id FROM probe_effects GROUP BY job_id HAVING count(*) > 1) d`},
	{"effect_not_completed", `SELECT count(*) FROM probe_effects e JOIN river_job j ON j.id = e.job_id WHERE j.state <> 'completed'`},
	{"completed_no_effect", `SELECT count(*) FROM river_job j WHERE j.kind NOT IN ('tick') AND j.state = 'completed'
		AND NOT EXISTS (SELECT 1 FROM probe_effects e WHERE e.job_id = j.id)`},
	// Scheduling: no attempt is fetched before its scheduled_at (250 ms covers host clock skew).
	{"early_fetch", `SELECT count(*) FROM probe_attempts WHERE attempted_at < scheduled_at - interval '250 ms'`},
	// Retries: attempts never exceed max_attempts; discard only after the last one.
	{"over_attempts", `SELECT count(*) FROM probe_attempts WHERE attempt > max_attempts`},
	{"early_discard", `SELECT count(*) FROM river_job WHERE state = 'discarded' AND kind <> 'uniq' AND attempt < max_attempts`},
	// Snooze: snoozes never exhaust max_attempts.
	{"snooze_exhausted", `SELECT count(*) FROM river_job WHERE kind = 'snooze' AND state <> 'completed'
		AND jsonb_array_length(coalesce(to_jsonb(errors), '[]')) < max_attempts`},
	// Cancel: no attempt is fetched after JobCancel returned, and a cancelled job never completes.
	{"attempt_after_cancel", `SELECT count(*) FROM probe_attempts a JOIN probe_cancels c ON c.job_id = a.job_id WHERE a.attempted_at > c.returned_at`},
	{"cancel_bad_final", `SELECT count(*) FROM probe_cancels c JOIN river_job j ON j.id = c.job_id WHERE j.state NOT IN ('cancelled','completed')
		AND NOT (j.state = 'discarded' AND j.attempt >= j.max_attempts)`},
	// Pause: once noticed (grace), no fetch in the paused queue until resume.
	{"fetch_while_paused", `SELECT count(*) FROM probe_attempts a JOIN probe_pauses p
		ON a.attempted_at > p.paused_at + interval '` + fmt.Sprint(pauseGrace.Seconds()) + ` s' AND a.attempted_at < p.resuming_at WHERE a.kind = 'pause'`},
	// Unique: one completed job per key; a discarded one only beside a covering duplicate.
	{"uniq_dup_completed", `SELECT count(*) FROM (SELECT unique_key FROM river_job WHERE kind = 'uniq' AND state = 'completed' GROUP BY 1 HAVING count(*) > 1) d`},
	{"uniq_bad_discard", `SELECT count(*) FROM river_job j WHERE j.kind = 'uniq' AND j.state = 'discarded' AND NOT EXISTS
		(SELECT 1 FROM river_job o WHERE o.unique_key = j.unique_key AND o.id <> j.id AND o.state IN ('available','completed','pending','running','scheduled'))`},
	// Periodic + unique per period.
	{"dup_tick", `SELECT count(*) FROM (SELECT date_bin('2 seconds', scheduled_at, 'epoch'::timestamptz) p FROM river_job WHERE kind = 'tick' GROUP BY 1 HAVING count(*) > 1) d`},
	{"dup_tick_effect", `SELECT count(*) FROM (SELECT job_id FROM probe_ticks GROUP BY job_id HAVING count(*) > 1) d`},
}

var info = []struct{ name, sql string }{
	{"jobs by kind/state", `SELECT string_agg(kind || ':' || state || '=' || n, ' ' ORDER BY kind, state) FROM (SELECT kind, state, count(*) n FROM river_job GROUP BY 1,2) s`},
	{"attempts logged", `SELECT count(*)::text FROM probe_attempts`},
	{"cancels / pauses", `SELECT (SELECT count(*) FROM probe_cancels) || ' / ' || (SELECT count(*) FROM probe_pauses)`},
	{"max pause lag", `SELECT coalesce(max(a.attempted_at - p.paused_at)::text, 'none') FROM probe_attempts a JOIN probe_pauses p
		ON a.attempted_at > p.paused_at AND a.attempted_at < p.resuming_at WHERE a.kind = 'pause'`},
	{"max snooze attempt", `SELECT coalesce(max(attempt)::text, '-') FROM probe_attempts WHERE kind = 'snooze'`},
}

func check(ctx context.Context, pool *pgxpool.Pool, seed int64, left int, commitErrs int64) int {
	var committed, rolled, faults int
	_ = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM probe_inserted), (SELECT count(*) FROM probe_rolledback), (SELECT count(*) FROM probe_faults)`).Scan(&committed, &rolled, &faults)
	fmt.Printf("seed=%d committed=%d rolledback=%d faults=%d ambiguous_commits=%d unfinished_at_quiesce=%d\n",
		seed, committed, rolled, faults, commitErrs, left)
	for _, q := range info {
		var s string
		_ = pool.QueryRow(ctx, q.sql).Scan(&s)
		fmt.Printf("  %-22s %s\n", q.name+":", s)
	}
	bad := 0
	results := map[string]int{}
	defer func() {
		if path := os.Getenv("PROBE_RESULT"); path != "" {
			data, _ := json.Marshal(results)
			_ = os.WriteFile(path, data, 0o644)
		}
	}()
	for _, q := range checks {
		var n int
		if err := pool.QueryRow(ctx, q.sql).Scan(&n); err != nil {
			fmt.Printf("  %-22s query error %v\n", q.name, err)
			results[q.name] = -1
			bad++
			continue
		}
		results[q.name] = n
		mark := "ok"
		if n > 0 {
			mark = "VIOLATION"
			bad++
		}
		fmt.Printf("  %-22s %6d  %s\n", q.name, n, mark)
	}
	return bad
}

func must(err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: riverprobe run|worker|check")
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	id := fs.String("id", "w1", "worker id")
	n := fs.Int("workers", 3, "Go workers")
	nJS := fs.Int("js", 0, "JS workers")
	nRuby := fs.Int("ruby", 0, "Ruby workers (never leaders)")
	nJava := fs.Int("java", 0, "Java workers")
	nRust := fs.Int("rust", 0, "Rust workers")
	dur := fs.Duration("duration", 60*time.Second, "fault phase")
	faults := fs.String("faults", "", "comma-separated faults")
	seed := fs.Int64("seed", 1, "seed")
	_ = fs.Parse(os.Args[2:])
	switch os.Args[1] {
	case "worker":
		worker(*id)
	case "uniqtest":
		pool, err := pgxpool.New(context.Background(), dsn)
		must(err)
		ctl, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
		must(err)
		var key int
		must(pool.QueryRow(context.Background(), `SELECT (args->>'key')::int FROM river_job WHERE id = $1`, *seed).Scan(&key))
		_, err = pool.Exec(context.Background(), `UPDATE river_job SET state = 'retryable', finalized_at = NULL, scheduled_at = now() + interval '1 hour' WHERE id = $1`, *seed)
		must(err)
		res, err := ctl.Insert(context.Background(), UniqArgs{Key: key, Seq: -1}, &river.InsertOpts{UniqueOpts: uniqOpts})
		must(err)
		fmt.Printf("key=%d skipped=%v new_id=%d existing_state=%s\n", key, res.UniqueSkippedAsDuplicate, res.Job.ID, res.Job.State)
		os.Exit(0)
	case "racerepro":
		os.Exit(raceRepro())
	case "check":
		pool, err := pgxpool.New(context.Background(), dsn)
		must(err)
		os.Exit(check(context.Background(), pool, *seed, -1, -1))
	case "run":
		var fl []string
		if *faults != "" {
			fl = strings.Split(*faults, ",")
		}
		if run(*n, *nJS, *nRuby, *nJava, *nRust, *dur, fl, *seed) > 0 {
			os.Exit(1)
		}
	}
}
