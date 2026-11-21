// A Java worker that is not a leader. On attempt 1 it overruns its timeout,
// ignoring the interrupt the timeout sends, as a worker stuck in a blocking
// call does, until the job is no longer running (a leader's rescuer has
// retried it). Every attempt then commits its effect
// and the job's completion in one transaction, with complete(connection).
import com.riverqueue.*;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class StallingWorker {
  record AuditedArgs(int seq) {}

  static String state(Database db, long id) throws Exception {
    try (Connection c = db.connection(); var s = c.prepareStatement("SELECT state FROM river_job WHERE id = ?")) {
      s.setLong(1, id);
      var rs = s.executeQuery();
      rs.next();
      return rs.getString(1);
    }
  }

  public static void main(String[] args) throws Exception {
    String dsn = System.getenv().getOrDefault("DATABASE_URL", "postgres://postgres:temper@postgres:5432/river");
    var db = Database.connect(dsn, "java-stalling");
    var audited = JobType.of("audited", AuditedArgs.class);
    var workers = new Workers().add(audited, ctx -> {
      var job = ctx.job();
      try (Connection c = db.connection(); var s = c.prepareStatement(
          "INSERT INTO audit_attempts (job_id, attempt, worker) VALUES (?, ?, 'java')")) {
        s.setLong(1, job.id());
        s.setInt(2, job.attempt());
        s.executeUpdate();
      }
      if (job.attempt() == 1) {
        while (state(db, job.id()).equals("running")) {
          try { Thread.sleep(100); } catch (InterruptedException ignored) { }
        }
        System.out.println("attempt 1 resumes; the job is now " + state(db, job.id()));
      }
      try (Connection c = db.connection()) {
        c.setAutoCommit(false);
        try (var s = c.prepareStatement("INSERT INTO audit_effects (job_id, attempt, worker) VALUES (?, ?, 'java')")) {
          s.setLong(1, job.id());
          s.setInt(2, job.attempt());
          s.executeUpdate();
        }
        var after = ctx.complete(c);
        c.commit();
        System.out.println("attempt " + job.attempt() + ": complete(connection) returned " + after.state() + " and the effect committed");
      }
    });
    var client = Client.builder(db)
        .id("java-stalling")
        .queue(Client.QUEUE_DEFAULT, 2)
        .workers(workers)
        .pollInterval(Duration.ofMillis(100))
        .jobTimeout(Duration.ofSeconds(1))
        .leadership(false)
        .stopTimeout(Duration.ofSeconds(1))
        .build();
    var stop = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown));
    try (var run = client.start()) {
      stop.await();
    }
  }
}
