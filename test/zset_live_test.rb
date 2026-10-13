# Live flow for the sorted-set commands, shaped like a scheduled-job
# store: members scored by a Float epoch, read back by score and rank.
# Spinel-only (committed .expected); oracle/zset_live.rb replays it
# through the real redis-rb gem, so the score spelling on the wire is
# held to redis-rb's.
#
# Requires redis-server + redis-cli on PATH.
require "redis"

module ZsetShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

ZSET_PORT = 16431

def zset_connect(port)
  attempts = 0
  while true
    begin
      c = Redis.new(url: "redis://127.0.0.1:" + port.to_s + "/1")   # assign-then-return: matz/spinel#1775
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "zset test: redis-server did not come up on " + port.to_s
      end
      ZsetShell.sp_net_shell_capture("sleep 0.1", 16)
    end
  end
end

ZsetShell.sp_net_shell_capture("redis-server --port 16431 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-zset.pid --logfile /tmp/spinel-redis-zset.log", 256)
r = zset_connect(ZSET_PORT)
r.flushdb

scores = [1791599858.8335917, 1791599558.332 + 300.5, 1791599558.0, 123456789.12345679, 0.1 + 0.2, 1791599558.9999998]
i = 0
scores.each do |s|
  puts "zadd_new     " + r.zadd("schedule", s, "job-" + i.to_s).to_s
  i = i + 1
end
puts "zadd_again   " + r.zadd("schedule", 42, "job-0").to_s
puts "zcard        " + r.zcard("schedule").to_s

i = 0
while i < scores.length
  got = r.zscore("schedule", "job-" + i.to_s)
  want = i == 0 ? 42.0 : scores[i]
  puts "zscore_" + i.to_s + "     " + (got == want).to_s + " " + got.to_s
  i = i + 1
end
m = r.zscore("schedule", "missing")
puts "zscore_nil   " + m.nil?.to_s

puts "zrange       " + r.zrange("schedule", 0, -1).join(",")
puts "zrange_top2  " + r.zrange("schedule", -2, -1).join(",")
puts "zrem         " + r.zrem("schedule", "job-4").to_s
puts "zrem_again   " + r.zrem("schedule", "job-4").to_s
puts "zcard_after  " + r.zcard("schedule").to_s

# a sorted-set command on a list key is a WRONGTYPE error
r.lpush("schedule:list", "x")
begin
  r.zadd("schedule:list", 1, "m")
  puts "wrongtype    added?!"
rescue Redis::WrongTypeError => e
  puts "wrongtype    " + e.is_a?(Redis::CommandError).to_s
end

r.close
ZsetShell.sp_net_shell_capture("redis-cli -p 16431 shutdown nosave 2>/dev/null", 64)
puts "done"
