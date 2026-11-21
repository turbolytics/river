# Ruby twin of the Go probe worker. Ruby clients cannot lead here: the Ruby
# leader rescues stuck jobs only after a fixed hour (docs/README.md), which
# would leave killed attempts stuck past the probe's quiesce.
require "riverqueue-sequel"
require "pg"

ID = ARGV.fetch(0)
DB = Sequel.connect(ENV.fetch("PROBE_DSN"), max_connections: 40, after_connect: ->(c) { c.exec("SET application_name = 'probe-#{ID}'") })

def record(job)
  r = job.row
  3.times do
    DB[:probe_attempts].insert(job_id: r.id, kind: r.kind, attempt: r.attempt, max_attempts: r.max_attempts,
      attempted_at: r.attempted_at, scheduled_at: r.scheduled_at, worker: ID)
    return
  rescue Sequel::Error, PG::Error
    sleep 0.1
  end
end

def complete(job, seq)
  DB.transaction do
    DB[:probe_effects].insert(job_id: job.row.id, kind: job.row.kind, seq: seq, attempt: job.row.attempt, worker: ID)
    job.client.job_complete_tx(job)
  end
end

class LedgerWorker
  def self.kind = "ledger"
  def work(job) = (record(job); sleep(rand(300) / 1000.0); complete(job, job.args["seq"]))
end

class FlakyWorker
  def self.kind = "flaky"
  def work(job)
    record(job)
    raise "planned failure #{job.row.attempt}" if job.row.attempt <= job.args["fail_first"]
    complete(job, job.args["seq"])
  end
end

class SnoozeWorker
  def self.kind = "snooze"
  def work(job)
    record(job)
    if DB[:probe_snoozes].where(job_id: job.row.id).count < job.args["snoozes"]
      DB[:probe_snoozes].insert(job_id: job.row.id)
      raise River.job_snooze((200 + rand(800)) / 1000.0)
    end
    complete(job, job.args["seq"])
  end
end

class CancelWorker
  def self.kind = "cancel"
  def work(job)
    record(job)
    sleep(rand(1200) / 1000.0) # remote cancellation interrupts this thread
    case rand(3)
    when 0 then raise River.job_snooze(0.3)
    when 1 then raise "planned failure"
    end
    complete(job, job.args["seq"])
  end
end

class PauseWorker
  def self.kind = "pause"
  def work(job) = (record(job); complete(job, job.args["seq"]))
end

class UniqWorker
  def self.kind = "uniq"
  def work(job)
    record(job)
    raise "planned first-attempt failure" if job.row.attempt == 1 && job.row.id.even?
    complete(job, job.args["seq"])
  end
end

class TickWorker
  def self.kind = "tick"
  def work(job)
    record(job)
    DB.transaction do
      DB[:probe_ticks].insert(job_id: job.row.id, scheduled_at: job.row.scheduled_at, worker: ID)
      job.client.job_complete_tx(job)
    end
  end
end

class ProbeRetry
  # 8 s for uniq: longer than the scheduler interval, so they wait in retryable.
  def next_retry(row, _error = nil, now: Time.now.utc) = now + (row.kind == "uniq" ? 8 : 1)
end

workers = River::Workers.new
[LedgerWorker, FlakyWorker, SnoozeWorker, CancelWorker, PauseWorker, UniqWorker, TickWorker].each { |w| workers.add(w) }
client = River::Client.new(River::Driver::Sequel.new(DB), config: River::Config.new(
  id: ID,
  queues: {
    default: River::QueueConfig.new(max_workers: 20, fetch_poll_interval: 0.1),
    pausable: River::QueueConfig.new(max_workers: 5, fetch_poll_interval: 0.1)
  },
  workers: workers,
  job_timeout: 2,
  retry_policy: ProbeRetry.new,
  leader_election_disabled: true
))

stop = Queue.new
Signal.trap("TERM") { stop << true }
client.start
stop.pop
client.stop
DB.disconnect
