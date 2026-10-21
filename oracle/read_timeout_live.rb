# Oracle twin of test/read_timeout_live_test.rb (real redis-rb gem): the
# same flow, with its own DEBUG-enabled server on the same private port.
require "redis"

TIMEOUT_PORT = 16440

def timeout_sh(cmd)
  system(cmd)
end

def timeout_await
  50.times do
    begin
      c = Redis.new(host: "127.0.0.1", port: TIMEOUT_PORT)
      c.ping
      return c
    rescue StandardError
      sleep 0.1
    end
  end
  raise "timeout oracle: redis-server did not come up on #{TIMEOUT_PORT}"
end

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

timeout_sh("redis-server --port 16440 --enable-debug-command yes --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-timeout-oracle.pid --logfile /tmp/spinel-redis-timeout-oracle.log")
admin = timeout_await
puts "ping         " + admin.ping

quick = Redis.new(host: "127.0.0.1", port: TIMEOUT_PORT, read_timeout: 2.0)
puts "in_time      " + quick.set("timeout:k", "v")
quick.close

# reconnect_attempts: 0 keeps redis-rb from retrying the command on a fresh
# connection, as spinel-redis does not reconnect
slow = Redis.new(host: "127.0.0.1", port: TIMEOUT_PORT, read_timeout: 0.5, reconnect_attempts: 0)
slow.ping
t0 = now
begin
  slow.call("DEBUG", "SLEEP", "2")
  puts "slow         answered?!"
rescue Redis::TimeoutError => e
  waited = now - t0
  puts "slow         TimeoutError " + e.is_a?(Redis::BaseConnectionError).to_s
  puts "waited       " + (waited >= 0.4 && waited < 1.9).to_s
end

patient = Redis.new(host: "127.0.0.1", port: TIMEOUT_PORT)
puts "no_deadline  " + patient.get("timeout:k").to_s
patient.del("timeout:k")
patient.close

timeout_sh("redis-cli -p 16440 shutdown nosave 2>/dev/null")
puts "done"
