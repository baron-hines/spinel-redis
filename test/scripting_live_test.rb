# Live flow for SCRIPT LOAD and EVALSHA, with the one-member pop a
# Sidekiq scheduler runs over its retry and schedule sets (Sidekiq
# 8.1.7's LUA_ZPOPBYSCORE, byte for byte): due members come back one at
# a time, then nil; after SCRIPT FLUSH, EVALSHA raises NoScriptError and
# a reload answers the same digest. Spinel-only (committed .expected);
# oracle/scripting_live.rb replays it through the real redis-rb gem.
#
# Requires redis-server + redis-cli on PATH.
require "redis"

module ScriptShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

SCRIPT_PORT = 16432

ZPOPBYSCORE = "local key, now = KEYS[1], ARGV[1]\n" \
  "local jobs = redis.call(\"zrange\", key, \"-inf\", now, \"byscore\", \"limit\", 0, 1)\n" \
  "if jobs[1] then\n" \
  "  redis.call(\"zrem\", key, jobs[1])\n" \
  "  return jobs[1]\n" \
  "end\n"

def script_connect(port)
  attempts = 0
  while true
    begin
      c = Redis.new(url: "redis://127.0.0.1:" + port.to_s + "/1")   # assign-then-return: matz/spinel#1775
      c.ping
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "scripting test: redis-server did not come up on " + port.to_s
      end
      ScriptShell.sp_net_shell_capture("sleep 0.1", 16)
    end
  end
end

ScriptShell.sp_net_shell_capture("redis-server --port 16432 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-script.pid --logfile /tmp/spinel-redis-script.log", 256)
r = script_connect(SCRIPT_PORT)
r.flushdb

sha = r.script(:load, ZPOPBYSCORE)
puts "sha          " + sha

r.zadd("retry", 200.5, "due-2")
r.zadd("retry", 100.25, "due-1")
r.zadd("retry", 9000000000.0, "later")
i = 0
while i < 3
  got = r.evalsha(sha, ["retry"], ["1791599558.5"])
  puts "pop_" + i.to_s + "        " + (got.nil? ? "nil" : got)
  i = i + 1
end
puts "left         " + r.zrange("retry", 0, -1).join(",")
puts "empty_set    " + r.evalsha(sha, ["schedule"], ["1791599558.5"]).nil?.to_s

puts "flushed      " + r.script(:flush)
kind = "none"
begin
  r.evalsha(sha, ["retry"], ["9999999999"])
rescue Redis::NoScriptError => e
  kind = "noscript " + e.is_a?(Redis::CommandError).to_s
end
puts "after_flush  " + kind
puts "reload_same  " + (r.script(:load, ZPOPBYSCORE) == sha).to_s
puts "pop_later    " + r.evalsha(sha, ["retry"], ["9999999999"]).to_s

r.close
ScriptShell.sp_net_shell_capture("redis-cli -p 16432 shutdown nosave 2>/dev/null", 64)
puts "done"
