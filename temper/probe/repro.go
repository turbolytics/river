package main

// raceRepro deterministically reproduces a JobCancel landing between the
// rescuer's read of stuck jobs and its update:
//
//  1. Jobs A, B and C sit in state running with an old attempted_at, as a
//     crashed worker leaves them, in a queue no client works yet.
//  2. C is cancelled first (the control): the rescuer reads the cancel flag.
//  3. A transaction holds A's row lock. The leader's rescuer reads A, B and C,
//     then blocks on A inside JobRescueMany.
//  4. JobCancel(B) commits while the rescuer is blocked, and returns running.
//  5. The lock is released. The rescuer writes the state it decided before
//     the cancel.
//  6. A worker on the queue starts. Does B run again?

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

func raceRepro() int {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	must(err)
	defer pool.Close()
	for _, q := range []string{
		`DROP SCHEMA public CASCADE`, `CREATE SCHEMA public`,
		`CREATE TABLE probe_effects (id bigserial, job_id bigint, kind text, seq bigint, attempt int, worker text, at timestamptz DEFAULT clock_timestamp())`,
		`CREATE TABLE probe_attempts (job_id bigint, kind text, attempt int, max_attempts int, attempted_at timestamptz, scheduled_at timestamptz, worker text, at timestamptz DEFAULT clock_timestamp())`,
	} {
		_, err := pool.Exec(ctx, q)
		must(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	must(err)
	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	must(err)

	insertOnly, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	must(err)
	ids := map[string]int64{}
	for i, name := range []string{"A", "B", "C"} {
		res, err := insertOnly.Insert(ctx, LedgerArgs{Seq: int64(i + 1)}, &river.InsertOpts{Queue: "parked"})
		must(err)
		ids[name] = res.Job.ID
	}
	_, err = pool.Exec(ctx, `UPDATE river_job SET state = 'running', attempt = 1,
		attempted_at = now() - interval '1 hour', attempted_by = '{crashed-worker}' WHERE queue = 'parked'`)
	must(err)
	fmt.Printf("1. A=%d B=%d C=%d are running with attempted_at an hour ago (a crashed worker)\n", ids["A"], ids["B"], ids["C"])

	row, err := insertOnly.JobCancel(ctx, ids["C"])
	must(err)
	fmt.Printf("2. JobCancel(C) before the rescuer runs: returned state %s\n", row.State)

	lockTx, err := pool.Begin(ctx)
	must(err)
	_, err = lockTx.Exec(ctx, `SELECT id FROM river_job WHERE id = $1 FOR UPDATE`, ids["A"])
	must(err)
	fmt.Println("3. a transaction holds A's row lock; starting a leader client")

	e := &env{pool: pool, id: "leader"}
	workers := river.NewWorkers()
	river.AddWorker(workers, &LedgerWorker{env: e})
	leaderCtx, stopLeader := context.WithCancel(ctx)
	defer stopLeader()
	leader, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		ID:                   "leader",
		Queues:               map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}},
		Workers:              workers,
		JobTimeout:           time.Second,
		RescueStuckJobsAfter: 2 * time.Second,
	})
	must(err)
	must(leader.Start(leaderCtx))

	blocked := false
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		var n int
		must(pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE query LIKE '%river:rescue_count%' AND query NOT LIKE '%pg_stat_activity%' AND wait_event_type = 'Lock'`).Scan(&n))
		if n > 0 {
			blocked = true
			break
		}
	}
	if !blocked {
		log.Print("the rescuer never blocked on A's lock")
		return 1
	}
	fmt.Println("   the rescuer has read A, B and C and is blocked on A inside JobRescueMany")

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	row, err = insertOnly.JobCancel(cctx, ids["B"])
	cancel()
	if err != nil {
		log.Printf("JobCancel(B) did not finish while the rescuer was blocked (it may have locked B first): %v", err)
		return 1
	}
	var returned time.Time
	must(pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&returned))
	fmt.Printf("4. JobCancel(B) returned state %s at %s\n", row.State, returned.Format("15:04:05.000"))

	must(lockTx.Commit(ctx))
	fmt.Println("5. A's lock released; the rescuer finishes its update")
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		var running int
		must(pool.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE queue = 'parked' AND state = 'running'`).Scan(&running))
		if running == 0 {
			break
		}
	}
	report := func() {
		rows, err := pool.Query(ctx, `SELECT id, state, attempt, metadata ? 'cancel_attempted_at', coalesce(array_length(errors, 1), 0)
			FROM river_job WHERE queue = 'parked' ORDER BY id`)
		must(err)
		defer rows.Close()
		for rows.Next() {
			var id int64
			var state string
			var attempt, nerr int
			var flagged bool
			must(rows.Scan(&id, &state, &attempt, &flagged, &nerr))
			name := map[int64]string{ids["A"]: "A", ids["B"]: "B", ids["C"]: "C"}[id]
			fmt.Printf("   %s: state=%-10s attempt=%d cancel_requested=%v errors=%d\n", name, state, attempt, flagged, nerr)
		}
	}
	report()

	fmt.Println("6. a worker starts on the parked queue")
	w := &env{pool: pool, id: "worker"}
	wWorkers := river.NewWorkers()
	river.AddWorker(wWorkers, &LedgerWorker{env: w})
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	worker, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		ID:                     "worker",
		Queues:                 map[string]river.QueueConfig{"parked": {MaxWorkers: 5}},
		Workers:                wWorkers,
		LeaderElectionDisabled: true,
	})
	must(err)
	must(worker.Start(workerCtx))
	// The leader's scheduler (every 5 s) moves retryable jobs to available.
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		var pending int
		must(pool.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE queue = 'parked' AND state IN ('retryable','available','running')`).Scan(&pending))
		if pending == 0 {
			break
		}
	}
	report()

	var ranAfter int
	var effects int
	must(pool.QueryRow(ctx, `SELECT count(*) FROM probe_attempts WHERE job_id = $1 AND attempted_at > $2`, ids["B"], returned).Scan(&ranAfter))
	must(pool.QueryRow(ctx, `SELECT count(*) FROM probe_effects WHERE job_id = $1`, ids["B"]).Scan(&effects))
	fmt.Printf("B: %d attempt(s) started after JobCancel(B) returned; %d committed effect(s)\n", ranAfter, effects)
	stopWorker()
	stopLeader()
	sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
	_ = worker.Stop(sctx)
	_ = leader.Stop(sctx)
	scancel()
	if ranAfter > 0 {
		fmt.Println("REPRODUCED: a job cancelled while running ran again after JobCancel returned")
		return 2
	}
	fmt.Println("not reproduced: B did not run again")
	return 0
}
