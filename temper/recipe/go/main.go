// Command rivercheck drives River for the fault scenarios in
// temper/scenarios. Each subcommand is one step a scenario runs; the checks
// themselves are SQL in the scenarios, over the audit tables this program
// writes.
//
//	rivercheck migrate                 migrate River and create the audit tables
//	rivercheck insert N                insert N audited jobs
//	rivercheck stuck N                 insert N audited jobs that look stuck: running, attempted an hour ago
//	rivercheck cancel ID               cancel a job and record what JobCancel returned
//	rivercheck work [flags]            work audited jobs until stopped
//	rivercheck periodic                lead, enqueueing a periodic job every 2s, unique per 2s
//	rivercheck periodic-audit [--explain]  count periodic runs a Go leader would enqueue twice
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

func dsn() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:temper@postgres:5432/river?sslmode=disable"
}

// AuditedArgs is a job whose every attempt and committed effect is recorded.
type AuditedArgs struct {
	Seq int `json:"seq"`
}

func (AuditedArgs) Kind() string { return "audited" }

// TickArgs is the periodic job.
type TickArgs struct{}

func (TickArgs) Kind() string { return "tick" }

// The periodic job runs every period and is unique per period, as River's
// docs show for periodic jobs that must not double up across leaders.
const period = 2 * time.Second

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		log.Fatal("usage: rivercheck migrate|insert|stuck|cancel|work|periodic|periodic-audit")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, dsn())
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "migrate":
		err = migrate(ctx, pool)
	case "insert", "stuck":
		err = insert(ctx, pool, atoi(args), cmd == "stuck")
	case "cancel":
		err = cancel(ctx, pool, int64(atoi(args)))
	case "work":
		err = work(ctx, pool, args)
	case "periodic":
		err = periodic(ctx, pool)
	case "periodic-audit":
		if len(args) == 1 && args[0] == "--explain" {
			log.SetOutput(os.Stdout)
		}
		var n int
		n, err = periodicAudit(ctx, pool)
		fmt.Println(n)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func atoi(args []string) int {
	if len(args) != 1 {
		log.Fatal("expected one number")
	}
	n, err := strconv.Atoi(args[0])
	if err != nil {
		log.Fatal(err)
	}
	return n
}

func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	m, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return err
	}
	if _, err := m.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
CREATE TABLE audit_attempts (job_id bigint, attempt int, worker text, at timestamptz DEFAULT clock_timestamp());
CREATE TABLE audit_effects  (job_id bigint, attempt int, worker text, at timestamptz DEFAULT clock_timestamp());
CREATE TABLE audit_cancels  (job_id bigint, returned_state text, at timestamptz DEFAULT clock_timestamp());`)
	return err
}

// inserter is a client that only inserts and cancels.
func inserter(pool *pgxpool.Pool) (*river.Client[pgx.Tx], error) {
	return river.NewClient(riverpgxv5.New(pool), &river.Config{})
}

func insert(ctx context.Context, pool *pgxpool.Pool, n int, stuck bool) error {
	c, err := inserter(pool)
	if err != nil {
		return err
	}
	for i := 1; i <= n; i++ {
		res, err := c.Insert(ctx, AuditedArgs{Seq: i}, nil)
		if err != nil {
			return err
		}
		if stuck {
			// What a client that died mid-job leaves behind: running, with an
			// attempt that started long ago.
			if _, err := pool.Exec(ctx, `UPDATE river_job SET state = 'running', attempt = 1,
				attempted_at = now() - interval '1 hour', attempted_by = ARRAY['gone'] WHERE id = $1`, res.Job.ID); err != nil {
				return err
			}
		}
		fmt.Println(res.Job.ID)
	}
	return nil
}

func cancel(ctx context.Context, pool *pgxpool.Pool, id int64) error {
	c, err := inserter(pool)
	if err != nil {
		return err
	}
	job, err := c.JobCancel(ctx, id)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `INSERT INTO audit_cancels (job_id, returned_state) VALUES ($1, $2)`, id, string(job.State))
	fmt.Printf("job %d: JobCancel returned %s\n", id, job.State)
	return err
}

// auditedWorker records each attempt as it starts, then commits the job's
// effect and its completion in one transaction (JobCompleteTx).
type auditedWorker struct {
	river.WorkerDefaults[AuditedArgs]
	pool *pgxpool.Pool
	id   string
}

func (w *auditedWorker) Work(ctx context.Context, job *river.Job[AuditedArgs]) error {
	if _, err := w.pool.Exec(ctx, `INSERT INTO audit_attempts (job_id, attempt, worker) VALUES ($1, $2, $3)`, job.ID, job.Attempt, w.id); err != nil {
		return err
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO audit_effects (job_id, attempt, worker) VALUES ($1, $2, $3)`, job.ID, job.Attempt, w.id); err != nil {
		return err
	}
	if _, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func work(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("work", flag.ExitOnError)
	queue := fs.String("queue", river.QueueDefault, "queue to work")
	rescueAfter := fs.Duration("rescue-after", time.Hour, "how long a job may run before the leader rescues it")
	id := fs.String("id", "go", "this client's id")
	_ = fs.Parse(args)

	workers := river.NewWorkers()
	river.AddWorker(workers, &auditedWorker{pool: pool, id: *id})
	timeout := time.Second
	if *rescueAfter < timeout {
		timeout = *rescueAfter
	}
	c, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		ID:                   *id,
		Queues:               map[string]river.QueueConfig{*queue: {MaxWorkers: 10}},
		Workers:              workers,
		FetchPollInterval:    100 * time.Millisecond,
		JobTimeout:           timeout,
		RescueStuckJobsAfter: *rescueAfter,
		RetryPolicy:          retryAfter{time.Second},
	})
	if err != nil {
		return err
	}
	return runUntilStopped(ctx, c)
}

// retryAfter retries every failed or rescued job after a fixed delay.
type retryAfter struct{ d time.Duration }

func (r retryAfter) NextRetry(job *rivertype.JobRow) time.Time { return time.Now().Add(r.d) }

type tickWorker struct {
	river.WorkerDefaults[TickArgs]
}

func (tickWorker) Work(context.Context, *river.Job[TickArgs]) error { return nil }

func periodic(ctx context.Context, pool *pgxpool.Pool) error {
	workers := river.NewWorkers()
	river.AddWorker(workers, tickWorker{})
	c, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		ID:      "go-periodic",
		Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 2}},
		Workers: workers,
		PeriodicJobs: []*river.PeriodicJob{river.NewPeriodicJob(
			river.PeriodicInterval(period),
			func() (river.JobArgs, *river.InsertOpts) {
				return TickArgs{}, &river.InsertOpts{UniqueOpts: river.UniqueOpts{ByPeriod: period}}
			},
			nil,
		)},
	})
	if err != nil {
		return err
	}
	return runUntilStopped(ctx, c)
}

func runUntilStopped(ctx context.Context, c *river.Client[pgx.Tx]) error {
	if err := c.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Stop(stopCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

// periodicAudit checks the enqueued periodic runs against River's rule for
// unique jobs: uniqueness by period is keyed on when the job is scheduled to
// run. For every run, it inserts the same run the way a Go leader enqueues it
// (the run time as ScheduledAt, unique per period), in a transaction it rolls
// back, so the audit changes nothing and can run twice. River must skip each
// as a duplicate. It prints how many it did not: runs that two leaders would
// both enqueue. Runs missing from the schedule are logged, not counted: the
// claim is at most once per period.
func periodicAudit(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	c, err := inserter(pool)
	if err != nil {
		return 0, err
	}
	rows, err := pool.Query(ctx, `SELECT id, scheduled_at, created_at, encode(unique_key, 'hex') FROM river_job WHERE kind = 'tick' ORDER BY scheduled_at`)
	if err != nil {
		return 0, err
	}
	type run struct {
		id                 int64
		scheduled, created time.Time
		key                string
	}
	var runs []run
	for rows.Next() {
		var r run
		if err := rows.Scan(&r.id, &r.scheduled, &r.created, &r.key); err != nil {
			return 0, err
		}
		runs = append(runs, r)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(runs) < 3 {
		return 0, fmt.Errorf("only %d periodic runs were enqueued; the leader never led", len(runs))
	}
	keyFor := func(t time.Time) string {
		sum := sha256.Sum256([]byte("&kind=tick&period=" + t.Truncate(period).UTC().Format(time.RFC3339)))
		return hex.EncodeToString(sum[:])
	}
	twice, never := 0, 0
	for _, r := range runs {
		keyedOn := "its run time"
		if r.key != keyFor(r.scheduled) {
			keyedOn = "NOT its run time"
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return 0, err
		}
		res, err := c.InsertTx(ctx, tx, TickArgs{}, &river.InsertOpts{ScheduledAt: r.scheduled, UniqueOpts: river.UniqueOpts{ByPeriod: period}})
		_ = tx.Rollback(ctx)
		if err != nil {
			return 0, err
		}
		outcome := "deduplicated"
		if !res.UniqueSkippedAsDuplicate {
			twice++
			outcome = "ENQUEUED AGAIN: not deduplicated"
		}
		log.Printf("run %d scheduled %s, inserted %s, unique key on %s; the same run from a Go leader: %s",
			r.id, r.scheduled.UTC().Format("15:04:05.000"), r.created.UTC().Format("15:04:05.000"), keyedOn, outcome)
	}
	for i := 1; i < len(runs); i++ {
		gap := runs[i].scheduled.Sub(runs[i-1].scheduled)
		if n := int((gap+period/2)/period) - 1; n > 0 {
			never += n
			log.Printf("%d run(s) due between %s and %s were never enqueued", n,
				runs[i-1].scheduled.UTC().Format("15:04:05.000"), runs[i].scheduled.UTC().Format("15:04:05.000"))
		}
	}
	log.Printf("%d runs: %d would be enqueued twice; %d never enqueued (not counted)", len(runs), twice, never)
	return twice, nil
}
