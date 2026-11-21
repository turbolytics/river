// Java twin of the Go probe worker: same kinds, effects and settings.
// Run: java -cp "/app/river/target/river-0.4.1.jar:/app/deps/*" Worker.java v1
import com.riverqueue.*;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public class Worker {
  record LedgerArgs(long seq) {}
  record FlakyArgs(long seq, int failFirst) {}
  record SnoozeArgs(long seq, int snoozes) {}
  record CancelArgs(long seq) {}
  record PauseArgs(long seq) {}
  record UniqArgs(int key, long seq) {}
  record TickArgs() {}

  static String id;
  static Database db;

  static int rand(int n) { return ThreadLocalRandom.current().nextInt(n); }

  // Best effort, like the Go worker: a missing row can hide a violation but never invent one.
  static void record(Job<?> job) {
    for (int i = 0; i < 3; i++) {
      try (Connection c = db.connection();
          var s = c.prepareStatement(
              "INSERT INTO probe_attempts (job_id, kind, attempt, max_attempts, attempted_at, scheduled_at, worker) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
        s.setLong(1, job.id());
        s.setString(2, job.kind());
        s.setInt(3, job.attempt());
        s.setInt(4, job.maxAttempts());
        s.setTimestamp(5, job.attemptedAt() == null ? null : Timestamp.from(job.attemptedAt()));
        s.setTimestamp(6, Timestamp.from(job.scheduledAt()));
        s.setString(7, id);
        s.executeUpdate();
        return;
      } catch (Exception e) {
        try { Thread.sleep(100); } catch (InterruptedException ie) { return; }
      }
    }
  }

  static <A> void complete(WorkContext<A> ctx, long seq) throws Exception {
    var job = ctx.job();
    try (Connection c = db.connection()) {
      c.setAutoCommit(false);
      try (var s = c.prepareStatement(
          "INSERT INTO probe_effects (job_id, kind, seq, attempt, worker) VALUES (?, ?, ?, ?, ?)")) {
        s.setLong(1, job.id());
        s.setString(2, job.kind());
        s.setLong(3, seq);
        s.setInt(4, job.attempt());
        s.setString(5, id);
        s.executeUpdate();
        ctx.complete(c);
        c.commit();
      } catch (Exception e) {
        c.rollback();
        throw e;
      }
    }
  }

  public static void main(String[] argv) throws Exception {
    id = argv[0];
    db = Database.connect(System.getenv("PROBE_DSN"), "probe-" + id);
    var ledger = JobType.of("ledger", LedgerArgs.class);
    var flaky = JobType.of("flaky", FlakyArgs.class);
    var snooze = JobType.of("snooze", SnoozeArgs.class);
    var cancel = JobType.of("cancel", CancelArgs.class);
    var pause = JobType.of("pause", PauseArgs.class);
    var uniq = JobType.of("uniq", UniqArgs.class);
    var tick = JobType.of("tick", TickArgs.class);
    var workers = new Workers()
        .add(ledger, ctx -> {
          record(ctx.job());
          Thread.sleep(rand(300));
          complete(ctx, ctx.args().seq());
        })
        .add(flaky, ctx -> {
          record(ctx.job());
          if (ctx.job().attempt() <= ctx.args().failFirst())
            throw new RuntimeException("planned failure " + ctx.job().attempt());
          complete(ctx, ctx.args().seq());
        })
        .add(snooze, ctx -> {
          record(ctx.job());
          long done;
          try (Connection c = db.connection();
              var s = c.prepareStatement("SELECT count(*) FROM probe_snoozes WHERE job_id = ?")) {
            s.setLong(1, ctx.job().id());
            var rs = s.executeQuery();
            rs.next();
            done = rs.getLong(1);
          }
          if (done < ctx.args().snoozes()) {
            try (Connection c = db.connection();
                var s = c.prepareStatement("INSERT INTO probe_snoozes (job_id) VALUES (?)")) {
              s.setLong(1, ctx.job().id());
              s.executeUpdate();
            }
            ctx.snooze(Duration.ofMillis(200 + rand(800)));
          }
          complete(ctx, ctx.args().seq());
        })
        .add(cancel, ctx -> {
          record(ctx.job());
          if (ctx.awaitCancellation(Duration.ofMillis(rand(1200)))) ctx.checkCancelled();
          switch (rand(3)) {
            case 0 -> ctx.snooze(Duration.ofMillis(300));
            case 1 -> throw new RuntimeException("planned failure");
            default -> complete(ctx, ctx.args().seq());
          }
        })
        .add(pause, ctx -> {
          record(ctx.job());
          complete(ctx, ctx.args().seq());
        })
        .add(uniq, ctx -> {
          record(ctx.job());
          if (ctx.job().attempt() == 1 && ctx.job().id() % 2 == 0)
            throw new RuntimeException("planned first-attempt failure");
          complete(ctx, ctx.args().seq());
        })
        .add(tick, ctx -> {
          record(ctx.job());
          try (Connection c = db.connection()) {
            c.setAutoCommit(false);
            try (var s = c.prepareStatement(
                "INSERT INTO probe_ticks (job_id, scheduled_at, worker) VALUES (?, ?, ?)")) {
              s.setLong(1, ctx.job().id());
              s.setTimestamp(2, Timestamp.from(ctx.job().scheduledAt()));
              s.setString(3, id);
              s.executeUpdate();
              ctx.complete(c);
              c.commit();
            } catch (Exception e) {
              c.rollback();
              throw e;
            }
          }
        });
    var client = Client.builder(db)
        .id(id)
        .queue(Client.QUEUE_DEFAULT, 20)
        .queue("pausable", 5)
        .workers(workers)
        .pollInterval(Duration.ofMillis(100))
        .jobTimeout(Duration.ofSeconds(2))
        .rescueAfter(Duration.ofSeconds(4))
        // 8 s for uniq: longer than the scheduler interval, so they wait in retryable.
        .retryPolicy(job -> job.kind().equals("uniq") ? Duration.ofSeconds(8) : Duration.ofSeconds(1))
        .periodic(new PeriodicJob<>(null, Schedule.every(Duration.ofSeconds(2)), tick, new TickArgs(),
            InsertOptions.builder().unique(Unique.none().per(Duration.ofSeconds(2))).build(), true))
        .build();
    var stop = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown));
    try (var run = client.start()) {
      stop.await();
    }
  }
}
