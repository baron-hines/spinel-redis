# Live flow for pipelined, shaped like a job push: SADD queues plus
# LPUSH queue:<name> in one round trip, a ZADD on its own, against a
# --requirepass server on db 2 through url:. Spinel-only (committed
# .expected); oracle/pipelined_live.rb replays it through redis-rb.
#
# Requires redis-server + redis-cli on PATH.
require "redis"

module PipeShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

PIPE_URL = "redis://:pipepw@127.0.0.1:16432/2"

# Bulk replies come back as binary Strings here and UTF-8 ones from
# redis-rb (README ledger), so the payload is compared byte for byte.
def same_bytes(a, b)
  if a.bytesize != b.bytesize
    return false
  end
  i = 0
  while i < a.bytesize
    if a.getbyte(i) != b.getbyte(i)
      return false
    end
    i = i + 1
  end
  true
end

def pipe_connect
  attempts = 0
  while true
    begin
      c = Redis.new(url: PIPE_URL)   # assign-then-return: matz/spinel#1775
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "pipelined test: redis-server did not come up on 16432"
      end
      PipeShell.sp_net_shell_capture("sleep 0.1", 16)
    end
  end
end

PipeShell.sp_net_shell_capture("redis-server --port 16432 --requirepass pipepw --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-pipe.pid --logfile /tmp/spinel-redis-pipe.log", 256)
r = pipe_connect
r.flushdb

# two pushes, each one round trip
job = "{\"class\":\"Sidekiq::ActiveJob::Wrapper\",\"queue\":\"default\",\"args\":[\"é ✓\"]}"
res = r.pipelined do |p|
  p.sadd("queues", "default")
  p.lpush("queue:default", job)
end
puts "push_1       " + res.length.to_s + " " + res[0].to_s + " " + res[1].to_s
res = r.pipelined do |p|
  p.sadd("queues", "default")
  p.lpush("queue:default", job)
end
puts "push_2       " + res[0].to_s + " " + res[1].to_s
puts "llen         " + r.llen("queue:default").to_s
puts "queues       " + r.smembers("queues").sort.join(",")
got = r.lrange("queue:default", 0, 0)
puts "job_intact   " + same_bytes(got[0], job).to_s

# a scheduled job: one ZADD, read back as the same double
at = 1791599858.8335917
res = r.pipelined do |p|
  p.zadd("schedule", at, job)
end
puts "zadd_piped   " + res[0].to_s
puts "zscore       " + (r.zscore("schedule", job) == at).to_s

# mixed replies
res = r.pipelined do |p|
  p.set("stat:processed", "0")
  p.incrby("stat:processed", 5)
  p.expire("stat:processed", 60)
  p.hset("worker:1", "busy", "0")
  p.del("stat:processed")
end
puts "mixed        " + res[0].to_s + " " + res[1].to_s + " " + res[2].to_s + " " + res[3].to_s + " " + res[4].to_s

# an error reply in the middle: raised after the rest ran
begin
  r.pipelined do |p|
    p.rpush("after:1", "a")
    p.lpush("queues", "not-a-list")
    p.rpush("after:2", "b")
  end
  puts "pipe_error   none?!"
rescue Redis::CommandError => e
  puts "pipe_error   " + e.is_a?(Redis::WrongTypeError).to_s
end
puts "rest_ran     " + r.llen("after:1").to_s + " " + r.llen("after:2").to_s
puts "in_sync      " + r.ping

# the server goes away mid-session: a connection error, not a command one
PipeShell.sp_net_shell_capture("redis-cli -p 16432 -a pipepw --no-auth-warning shutdown nosave 2>/dev/null", 64)
PipeShell.sp_net_shell_capture("sleep 0.3", 16)
begin
  r.pipelined do |p|
    p.sadd("queues", "default")
    p.lpush("queue:default", job)
  end
  puts "after_kill   pushed?!"
rescue Redis::CommandError
  puts "after_kill   command error?!"
rescue Redis::BaseConnectionError
  puts "after_kill   connection error"
end
puts "done"
