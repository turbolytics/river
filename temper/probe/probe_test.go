package main

// The Temper map's tests. Each cluster runs the probe under every fault, and
// each invariant is claimed for that cluster from the probe's checks.
//
//	temper run --level faults -- sh -c 'cd temper/probe && GOWORK=off go test -timeout 4h'
//
// PROBE_SUBJECTS limits the clusters (comma-separated ids, such as
// cluster.go,cluster.js) and PROBE_DURATION sets each fault phase (default 150s).

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbolytics/temper"
)

var probeBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "riverprobe")
	if err != nil {
		panic(err)
	}
	probeBin = filepath.Join(dir, "riverprobe")
	build := exec.Command("go", "build", "-o", probeBin, ".")
	build.Env = append(os.Environ(), "GOWORK=off")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		panic(err)
	}
	code := temper.Run(m)
	os.RemoveAll(dir)
	os.Exit(code)
}

// clusters are the subjects: which clients share the database.
var clusters = []struct {
	id                        string
	goN, js, ruby, java, rust string
	seed                      string
}{
	{"cluster.go", "3", "0", "0", "0", "0", "70"},
	{"cluster.js", "0", "3", "0", "0", "0", "71"},
	{"cluster.go_js", "2", "2", "0", "0", "0", "72"},
	{"cluster.go_ruby", "2", "0", "2", "0", "0", "73"},
	{"cluster.go_java", "2", "0", "0", "2", "0", "74"},
	{"cluster.rust", "0", "0", "0", "0", "3", "75"},
	{"cluster.all", "1", "1", "1", "2", "1", "76"},
}

// invariants maps each invariant to the probe checks that must all be zero,
// and to the feature the scenario exercises.
var invariants = []struct {
	id, feature string
	checks      []string
}{
	{"job.never_lost", "job.insert_tx", []string{"lost", "lost_row", "unfinished"}},
	{"insert.rollback_never_runs", "job.insert_tx", []string{"phantom"}},
	{"complete_tx.effect_once", "job.complete_tx", []string{"dup_effect"}},
	{"complete_tx.effect_iff_completed", "job.complete_tx", []string{"effect_not_completed", "completed_no_effect"}},
	{"schedule.never_early", "job.schedule", []string{"early_fetch"}},
	{"retry.within_max_attempts", "job.retry", []string{"over_attempts"}},
	{"retry.discard_only_at_max", "leader.rescue", []string{"early_discard"}},
	{"snooze.never_exhausts", "job.snooze", []string{"snooze_exhausted"}},
	{"cancel.never_reruns", "job.cancel", []string{"attempt_after_cancel", "cancel_bad_final"}},
	{"pause.honored", "queue.pause", []string{"fetch_while_paused"}},
	{"unique.one_per_key", "job.unique", []string{"uniq_dup_completed", "uniq_bad_discard"}},
	{"periodic.once_per_period", "job.periodic", []string{"dup_tick", "dup_tick_effect"}},
}

func selected(id string) bool {
	want := os.Getenv("PROBE_SUBJECTS")
	if want == "" {
		return true
	}
	for _, s := range strings.Split(want, ",") {
		if strings.TrimSpace(s) == id {
			return true
		}
	}
	return false
}

func TestClusterUnderFaults(t *testing.T) {
	duration := os.Getenv("PROBE_DURATION")
	if duration == "" {
		duration = "150s"
	}
	for _, c := range clusters {
		if !selected(c.id) {
			continue
		}
		t.Run(c.id, func(t *testing.T) {
			result := filepath.Join(t.TempDir(), "result.json")
			cmd := exec.Command(probeBin, "run", "-workers", c.goN, "-js", c.js, "-ruby", c.ruby, "-java", c.java,
				"-rust", c.rust, "-duration", duration, "-seed", c.seed,
				"-faults", "kill,stop,terminate,pgcrash,pgpause,pgrestart")
			cmd.Env = append(os.Environ(), "PROBE_RESULT="+result)
			out, _ := cmd.CombinedOutput() // a violation exits 1; the result file says which
			t.Logf("probe output:\n%s", tail(string(out), 40))
			data, err := os.ReadFile(result)
			if err != nil {
				t.Fatalf("the probe wrote no result: %v", err)
			}
			var counts map[string]int
			if err := json.Unmarshal(data, &counts); err != nil {
				t.Fatal(err)
			}
			for _, inv := range invariants {
				t.Run(inv.id, func(t *testing.T) {
					temper.Covers(t, inv.id, temper.Subject(c.id))
					temper.Covers(t, inv.feature)
					temper.Covers(t, "clients.shared_database")
					for _, check := range inv.checks {
						n, ok := counts[check]
						if !ok {
							t.Errorf("%s: the probe did not report it", check)
						} else if n != 0 {
							t.Errorf("%s: %d", check, n)
						}
					}
				})
			}
		})
	}
}

// The rescuer reads stuck jobs, then updates them with the state it decided.
// A JobCancel that commits in between is overwritten: the job is retried and
// runs again. repro.go pins the timing with a row lock, using only the public
// client API.
func TestCancelDuringRescueNeverReruns(t *testing.T) {
	if !selected("cluster.go") {
		t.Skip("cluster.go not selected")
	}
	temper.Covers(t, "cancel.never_reruns", temper.Subject("cluster.go"))
	temper.Covers(t, "job.cancel")
	temper.Covers(t, "leader.rescue")
	out, err := exec.Command(probeBin, "racerepro").CombinedOutput()
	t.Logf("racerepro:\n%s", out)
	if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 2 {
		t.Fatal("a job cancelled while running ran again after JobCancel returned")
	} else if err != nil {
		t.Fatalf("racerepro did not run: %v", err)
	}
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
