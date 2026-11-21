// A Java client that leads and rescues, on a queue no job uses, so it works
// nothing itself: the Java twin of rivercheck's rescuer. Its rescuer retries
// a job that runs past 2 s. (A Java client's rescuer skips jobs it is running
// itself, so rescuing another client's stuck attempt takes a second client.)
import com.riverqueue.*;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class RescuerLeader {
  record AuditedArgs(int seq) {}

  public static void main(String[] args) throws Exception {
    String dsn = System.getenv().getOrDefault("DATABASE_URL", "postgres://postgres:temper@postgres:5432/river");
    var db = Database.connect(dsn, "java-rescuer");
    var audited = JobType.of("audited", AuditedArgs.class);
    var client = Client.builder(db)
        .id("java-rescuer")
        .queue("other", 1)
        .workers(new Workers().add(audited, ctx -> { }))
        .pollInterval(Duration.ofMillis(100))
        .jobTimeout(Duration.ofSeconds(1))
        .rescueAfter(Duration.ofSeconds(2))
        .build();
    var stop = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown));
    try (var run = client.start()) {
      stop.await();
    }
  }
}
