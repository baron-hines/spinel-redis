# Oracle twin of test/worker_live_test.rb (real redis-rb gem): the same
# flow, with its own DEBUG-enabled server on the same private port.
# redis-rb's brpop returns nil when its timeout passes; spinel-redis
# returns [], so the twin reads nil as [].
require "redis"
require "socket"

WORKER_PORT = 16450

def worker_sh(cmd)
  system(cmd)
end

def worker_await
  50.times do
    begin
      c = Redis.new(host: "127.0.0.1", port: WORKER_PORT)
      c.ping
      return c
    rescue StandardError
      sleep 0.1
    end
  end
  raise "worker oracle: redis-server did not come up on #{WORKER_PORT}"
end

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

worker_sh("redis-server --port 16450 --enable-debug-command yes --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-worker-oracle.pid --logfile /tmp/spinel-redis-worker-oracle.log")
admin = worker_await
admin.flushdb
puts "ping         " + admin.ping

# reconnect_attempts: 0 keeps redis-rb from retrying the command on a fresh
# connection, as spinel-redis does not reconnect
r = Redis.new(host: "127.0.0.1", port: WORKER_PORT, read_timeout: 0.5, reconnect_attempts: 0)
puts "rpush_one    " + r.rpush("queue:high", "h1").to_s
puts "rpush_many   " + r.rpush("queue:low", ["l1", "l2", "l3"]).to_s
puts "lrange       " + r.lrange("queue:low", 0, -1).join(",")

queues = ["queue:high", "queue:low"]
4.times do |i|
  puts "brpop_" + i.to_s + "      " + (r.brpop(queues, timeout: 1) || []).join(" ")
end

t0 = now
v = r.brpop(queues, timeout: 1) || []
waited = now - t0
puts "brpop_empty  " + v.length.to_s
puts "waited       " + (waited >= 0.9 && waited < 1.45).to_s
puts "after        " + r.ping

# DEBUG SLEEP goes out on a raw socket so this side doesn't wait for it
raw = TCPSocket.new("127.0.0.1", WORKER_PORT)
raw.write("*3\r\n$5\r\nDEBUG\r\n$5\r\nSLEEP\r\n$1\r\n3\r\n")
t0 = now
begin
  r.brpop(queues, timeout: 1)
  puts "stalled      answered?!"
rescue Redis::TimeoutError => e
  waited = now - t0
  puts "stalled      TimeoutError " + e.is_a?(Redis::BaseConnectionError).to_s
  puts "waited       " + (waited >= 1.3 && waited < 2.9).to_s
end
puts "slept        " + raw.gets.chomp.delete_prefix("+")
raw.close

admin.zadd("dead", 100, "j0")
admin.zadd("dead", 200, "j1")
admin.zadd("dead", 300, "j2")
admin.zadd("dead", 400, "j3")
admin.zadd("dead", 500, "j4")
puts "zrem_score   " + admin.zremrangebyscore("dead", "-inf", 200).to_s
puts "zrem_open    " + admin.zremrangebyscore("dead", "(500", "+inf").to_s
puts "zrem_rank    " + admin.zremrangebyrank("dead", 0, -3).to_s
puts "zrange       " + admin.zrange("dead", 0, -1).join(",")

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
puts "brpop_piped  " + (admin.brpop(["queue:piped"], timeout: 1) || []).join(" ")

admin.flushdb
admin.close
worker_sh("redis-cli -p 16450 shutdown nosave 2>/dev/null")
puts "done"
