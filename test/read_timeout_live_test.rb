# Live flow for read_timeout:, against a test-owned redis-server that
# allows DEBUG SLEEP. Spinel-only (the committed .expected keeps spin test
# from running it under CRuby); oracle/read_timeout_live.rb replays it
# through the real redis-rb gem.
#
# Requires redis-server + redis-cli on PATH.
require "redis"

module TimeoutShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

TIMEOUT_PORT = 16440

def timeout_sh(cmd)
  TimeoutShell.sp_net_shell_capture(cmd, 256)
end

def timeout_await
  attempts = 0
  while true
    begin
      c = Redis.new("127.0.0.1", TIMEOUT_PORT)   # assign-then-return: matz/spinel#1775
      c.ping
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "timeout test: redis-server did not come up on " + TIMEOUT_PORT.to_s
      end
      timeout_sh("sleep 0.1")
    end
  end
end

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

timeout_sh("redis-server --port 16440 --enable-debug-command yes --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-timeout.pid --logfile /tmp/spinel-redis-timeout.log")
admin = timeout_await
puts "ping         " + admin.ping

# a reply inside the deadline is read as usual
quick = Redis.new("127.0.0.1", TIMEOUT_PORT, read_timeout: 2.0)
puts "in_time      " + quick.set("timeout:k", "v")
quick.close

# the server takes longer than read_timeout: TimeoutError, a connection error
slow = Redis.new("127.0.0.1", TIMEOUT_PORT, read_timeout: 0.5)
t0 = now
begin
  slow.call_argv(["DEBUG", "SLEEP", "2"])
  puts "slow         answered?!"
rescue Redis::TimeoutError => e
  waited = now - t0
  puts "slow         TimeoutError " + e.is_a?(Redis::BaseConnectionError).to_s
  puts "waited       " + (waited >= 0.4 && waited < 1.9).to_s
end

# no read_timeout waits for the reply
patient = Redis.new("127.0.0.1", TIMEOUT_PORT)
puts "no_deadline  " + patient.get("timeout:k").to_s
patient.del("timeout:k")
patient.close

timeout_sh("redis-cli -p 16440 shutdown nosave 2>/dev/null")
puts "done"
