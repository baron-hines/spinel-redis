# Live flow for the commands a Sidekiq-style worker runs: BRPOP across
# queues in priority order, its read deadline, multi-value RPUSH, and
# trimming a sorted set by score and by rank. Spinel-only (the committed
# .expected keeps spin test from running it under CRuby);
# oracle/worker_live.rb replays it through the real redis-rb gem.
#
# Requires redis-server + redis-cli on PATH.
require "redis"

module WorkerShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

WORKER_PORT = 16450

def worker_sh(cmd)
  WorkerShell.sp_net_shell_capture(cmd, 256)
end

def worker_await
  attempts = 0
  while true
    begin
      c = Redis.new("127.0.0.1", WORKER_PORT)   # assign-then-return: matz/spinel#1775
      c.ping
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "worker test: redis-server did not come up on " + WORKER_PORT.to_s
      end
      worker_sh("sleep 0.1")
    end
  end
end

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

worker_sh("redis-server --port 16450 --enable-debug-command yes --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-worker.pid --logfile /tmp/spinel-redis-worker.log")
admin = worker_await
admin.flushdb
puts "ping         " + admin.ping

# RPUSH takes one value or several
r = Redis.new("127.0.0.1", WORKER_PORT, read_timeout: 0.5)
puts "rpush_one    " + r.rpush("queue:high", "h1").to_s
puts "rpush_many   " + r.rpush("queue:low", ["l1", "l2", "l3"]).to_s
puts "lrange       " + r.lrange("queue:low", 0, -1).join(",")

# BRPOP takes from the first key with an element, from the tail
queues = ["queue:high", "queue:low"]
i = 0
while i < 4
  puts "brpop_" + i.to_s + "      " + r.brpop(queues, timeout: 1).join(" ")
  i = i + 1
end

# empty queues: BRPOP's own timeout passes. The read deadline is 1 + 0.5
# seconds, so the 0.5 s read_timeout alone doesn't cut it short
t0 = now
v = r.brpop(queues, timeout: 1)
waited = now - t0
puts "brpop_empty  " + v.length.to_s
puts "waited       " + (waited >= 0.9 && waited < 1.45).to_s
puts "after        " + r.ping

# a server that stops answering: DEBUG SLEEP holds it for 3 s, so the
# read gives up at 1 + 0.5 s with a TimeoutError
admin.send_bytes(Resp.encode_command(["DEBUG", "SLEEP", "3"]))
t0 = now
begin
  r.brpop(queues, timeout: 1)
  puts "stalled      answered?!"
rescue Redis::TimeoutError => e
  waited = now - t0
  puts "stalled      TimeoutError " + e.is_a?(Redis::BaseConnectionError).to_s
  puts "waited       " + (waited >= 1.3 && waited < 2.9).to_s
end
puts "slept        " + admin.read_reply.str

# the dead-set trim: by score up to a cutoff, then by rank to a cap
admin.zadd("dead", 100, "j0")
admin.zadd("dead", 200, "j1")
admin.zadd("dead", 300, "j2")
admin.zadd("dead", 400, "j3")
admin.zadd("dead", 500, "j4")
puts "zrem_score   " + admin.zremrangebyscore("dead", "-inf", 200).to_s
puts "zrem_open    " + admin.zremrangebyscore("dead", "(500", "+inf").to_s
puts "zrem_rank    " + admin.zremrangebyrank("dead", 0, -3).to_s
puts "zrange       " + admin.zrange("dead", 0, -1).join(",")

# the same commands in a pipeline
# a new queue: the stalled BRPOP above is still parked on the server for
# its own second, and would take a value pushed onto queue:low
admin.zadd("dead", 600, "j5")
res = admin.pipelined do |p|
  p.rpush("queue:piped", ["p1", "p2"])
  p.zremrangebyscore("dead", "-inf", 300)
  p.zremrangebyrank("dead", 0, -2)
end
puts "pipelined    " + res.join(",")
puts "zrange_after " + admin.zrange("dead", 0, -1).join(",")
puts "brpop_piped  " + admin.brpop(["queue:piped"], timeout: 1).join(" ")

admin.flushdb
admin.close
worker_sh("redis-cli -p 16450 shutdown nosave 2>/dev/null")
puts "done"
