# connect_timeout: against a listener that never accepts. Once its accept
# queue is full the kernel drops new SYNs, so a connect hangs as one to a
# dead host does. Spinel-only (the committed .expected keeps spin test from
# running it under CRuby); oracle/connect_timeout.rb replays it through the
# real redis-rb gem. Needs no redis-server.
require "redis"

module StuckListener
  ffi_func :sp_net_listen_host,          [:str, :int, :int],    :int
  ffi_func :sp_net_local_port,           [:int],                :int
  ffi_func :sp_net_connect_timeout,      [:str, :int, :double], :int
  ffi_func :sp_net_take_connect_timeout, [],                    :int
  ffi_func :sp_net_close,                [:int],                :int
end

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

lfd = StuckListener.sp_net_listen_host("127.0.0.1", 0, 1)
port = StuckListener.sp_net_local_port(lfd)

# fill the accept queue: connect until one is no longer answered
held = []
fd = 0
tries = 0
while fd >= 0 && tries < 8
  fd = StuckListener.sp_net_connect_timeout("127.0.0.1", port, 0.3)
  held.push(fd) if fd >= 0
  tries = tries + 1
end
StuckListener.sp_net_take_connect_timeout
puts "queue_full   " + (fd < 0).to_s

# the connect runs out of connect_timeout: CannotConnectError, as redis-rb
t0 = now
begin
  r = Redis.new("127.0.0.1", port, connect_timeout: 0.5)
  r.ping
  puts "hung         connected?!"
rescue Redis::CannotConnectError => e
  waited = now - t0
  puts "hung         CannotConnectError " + e.is_a?(Redis::BaseConnectionError).to_s
  puts "waited       " + (waited >= 0.4 && waited < 1.9).to_s
end

held.each { |h| StuckListener.sp_net_close(h) }
StuckListener.sp_net_close(lfd)

# nothing listens now: refused at once, also CannotConnectError
t0 = now
begin
  r = Redis.new("127.0.0.1", port, connect_timeout: 5.0)
  r.ping
  puts "refused      connected?!"
rescue Redis::CannotConnectError => e
  puts "refused      CannotConnectError " + (now - t0 < 1.0).to_s
end
puts "done"
