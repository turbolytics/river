// A Java client that leads and enqueues a periodic job every 2 s, unique per
// 2 s: the same definition rivercheck periodic gives a Go leader.
import com.riverqueue.*;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class PeriodicLeader {
  record TickArgs() {}

  public static void main(String[] args) throws Exception {
    String dsn = System.getenv().getOrDefault("DATABASE_URL", "postgres://postgres:temper@postgres:5432/river");
    var db = Database.connect(dsn, "java-periodic");
    var tick = JobType.of("tick", TickArgs.class);
    var client = Client.builder(db)
        .id("java-periodic")
        .queue(Client.QUEUE_DEFAULT, 2)
        .workers(new Workers().add(tick, ctx -> { }))
        .pollInterval(Duration.ofMillis(100))
        .periodic(new PeriodicJob<>(null, Schedule.every(Duration.ofSeconds(2)), tick, new TickArgs(),
            InsertOptions.builder().unique(Unique.none().per(Duration.ofSeconds(2))).build(), false))
        .build();
    var stop = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown));
    try (var run = client.start()) {
      stop.await();
    }
  }
}
