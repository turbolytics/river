# Temper map for River

This map checks River's documented guarantees under faults, with every
language's client in one database. The probe and its tests are in
`temper/probe`.

## Reproduce the findings

Each finding is a scenario in `temper/scenarios/repro.yml`. One command
reproduces it, with Docker and Temper v0.1.6 or later installed
(`brew install turbolytics/tap/temper`, or
`go install github.com/turbolytics/temper/cmd/temper@v0.1.6`):

```sh
temper scenarios run --scenario river.cancel_during_rescue --build
temper scenarios run --scenario river.java_complete_after_rescue
temper scenarios run --scenario river.java_complete_after_rescue_java_only
temper scenarios run --scenario river.periodic_once_per_period
```

`--build` builds the images from this checkout (`temper/compose.yml`) first.
Later runs reuse them. Each scenario runs against a fresh Postgres, prints
its verdict, and exits 1 when the bug reproduces. Each failed 10 runs out of
10 with Temper v0.1.6. The full log is in
`.temper/scenarios/<scenario>/<subject>/run.log`. It holds every step, every
query result and every program's stderr, so the periodic audit's line for
each run is there too.

The queries and the held row lock use Temper's built-in `postgres` recipe
pack (`temper.yml`). The `rivercheck` steps are a named command in
`temper/commands.yml`.

| Scenario | What it shows | Affected |
|---|---|---|
| `river.cancel_during_rescue` | A job cancelled while the rescuer is retrying it runs again. | Go |
| `river.java_complete_after_rescue` | `complete(connection)` commits for an attempt the rescuer already retried, so the effect commits twice. | Java |
| `river.java_complete_after_rescue_java_only` | The same, with Java clients only: a stalling Java worker and a separate Java leader. (A Java client's rescuer skips jobs it is running itself, so the rescue needs a second client.) | Java |
| `river.periodic_once_per_period` | A periodic job unique per period is keyed on its insert time, not its run time, so a run enqueued late is not deduplicated. | Go and Java |

The programs the scenarios drive are in `temper/recipe`: `rivercheck` (Go)
and two Java programs. The checks are SQL over the audit tables they write.

`temper scenarios run` alone records nothing. To put the scenarios'
verdicts in the matrix beside the probe's, record them at the `faults` level:

```sh
temper run --level faults -- temper scenarios run
temper matrix --all-levels
```

## Setup

```sh
docker run -d --name temper-river-pg --memory 1g -p 26532:5432 \
  -e POSTGRES_PASSWORD=temper -e POSTGRES_DB=river postgres:17
```

The JS, Ruby, Java and Rust workers run in containers off Docker volumes
built from this checkout:

```sh
# JS: node 26, pnpm
docker volume create river-js
(cd js && tar -cf - --exclude node_modules .) | docker run --rm -i -v river-js:/app -w /app node:26 \
  sh -c 'tar -xf - && npm i -g pnpm@10.22.0 && CI=1 pnpm install --frozen-lockfile && pnpm run build:all'
# Ruby: ruby 3.4, the Sequel driver
docker volume create river-ruby
(cd ruby && tar -cf - --exclude vendor .) | docker run --rm -i -v river-ruby:/app -v $PWD/temper/probe/ruby:/probe:ro ruby:3.4 \
  sh -c 'tar -xf - -C /app && mkdir -p /app/probe-gems && cp /probe/Gemfile /app/probe-gems/ && \
    BUNDLE_GEMFILE=/app/probe-gems/Gemfile BUNDLE_PATH=/app/.gems bundle install'
# Java: JDK 21
docker volume create river-java
(cd java && tar -cf - --exclude target .) | docker run --rm -i -v river-java:/app -w /app maven:3-eclipse-temurin-21 \
  sh -c 'tar -xf - && mvn -q -B -DskipTests package && mvn -q -B -pl river dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/app/deps'
# Rust
docker volume create river-rust
(cd rust && tar -cf - --exclude target .) | docker run --rm -i -v river-rust:/app rust:1 sh -c 'tar -xf - -C /app'
(cd temper/probe/rust && tar -cf - .) | docker run --rm -i -v river-rust:/app -e CARGO_HOME=/app/.cargo rust:1 \
  sh -c 'mkdir -p /app/probe && tar -xf - -C /app/probe && cd /app/probe && cargo build --release'
```

Then run the map:

```sh
temper clean
temper run --level faults -- sh -c 'cd temper/probe && GOWORK=off go test -timeout 4h'
temper matrix --all-levels
```

Each cluster takes about five minutes: a 150 s fault phase and a drain.

## What the probe does

Workers in each cluster work seven job kinds while four inserters add jobs in
transactions, a quarter of them rolled back. A controller cancels half the
`cancel` jobs and pauses a queue, every other pause for 80 s. Faults hit the
workers (SIGKILL, a pause past the leader TTL, backend termination) and
Postgres (restart, crash, pause). After the fault phase, the probe waits while
jobs still finish, then checks the database:

| Kind | Tests |
|---|---|
| `ledger` | at-least-once and transactional enqueue; a fifth are scheduled in the future |
| `flaky` | fails its first N attempts, with max_attempts 1 to 4 |
| `snooze` | snoozes 4 times with max_attempts 2 |
| `cancel` | long attempts that snooze, error or complete, while JobCancel lands |
| `pause` | runs in the queue the controller pauses |
| `uniq` | unique by args without `retryable`; half fail their first attempt and wait 8 s, so duplicates collide with retries |
| `tick` | periodic every 2 s, unique per period, run on start |

Every job's effect commits in the same transaction as its completion
(JobCompleteTx or the port's equivalent). Every attempt is logged with River's
own `attempted_at` and `scheduled_at`, so timing checks use the database's
clock.

`TestCancelDuringRescueNeverReruns` pins one race deterministically with only
the public client API. The leader's rescuer reads stuck jobs, then updates them
with the state it decided. A row lock holds the rescuer between the two, while
JobCancel commits for another stuck job. The rescuer then retries the cancelled
job, and it runs again. `go run . racerepro` in `temper/probe` shows it.

## Notes

- Ruby clients run with leader election disabled. The Ruby leader rescues
  stuck jobs only after a fixed hour (`ruby/docs/README.md`), longer than the
  probe's drain. Ruby's rescuer locks each stuck job before it reads the cancel
  flag, so it does not have the race above.
- A pause sent while a client's LISTEN connection is down reaches it through a
  poll every minute (#1582). Go clients took up to 58 s. The check allows 65 s.
- Retries shorter than the scheduler's 5 s interval go straight to
  `available`, a unique state. The `uniq` jobs retry after 8 s so the collision
  path runs.
