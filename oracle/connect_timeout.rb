# Oracle twin of test/connect_timeout_test.rb (real redis-rb gem): the same
# flow against a listener that never accepts. Needs no redis-server.
require "redis"
require "socket"

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

srv = Socket.new(:INET, :STREAM)
srv.bind(Addrinfo.tcp("127.0.0.1", 0))
srv.listen(1)
port = srv.local_address.ip_port

# fill the accept queue: connect until one is no longer answered
held = []
full = false
8.times do
  begin
    held << Socket.tcp("127.0.0.1", port, connect_timeout: 0.3)
  rescue Errno::ETIMEDOUT, IO::TimeoutError
    full = true
    break
  end
end
puts "queue_full   " + full.to_s

# reconnect_attempts: 0, as spinel-redis does not reconnect
t0 = now
begin
  r = Redis.new(host: "127.0.0.1", port: port, connect_timeout: 0.5, reconnect_attempts: 0)
  r.ping
  puts "hung         connected?!"
rescue Redis::CannotConnectError => e
  waited = now - t0
  puts "hung         CannotConnectError " + e.is_a?(Redis::BaseConnectionError).to_s
  puts "waited       " + (waited >= 0.4 && waited < 1.9).to_s
end

held.each(&:close)
srv.close

t0 = now
begin
  r = Redis.new(host: "127.0.0.1", port: port, connect_timeout: 5.0, reconnect_attempts: 0)
  r.ping
  puts "refused      connected?!"
rescue Redis::CannotConnectError => e
  puts "refused      CannotConnectError " + (now - t0 < 1.0).to_s
end
puts "done"
