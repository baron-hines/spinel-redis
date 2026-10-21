# The real transport: a TCP connection to a Redis server over sp_net.
# Matches the transport duck RedisClientCore expects (write / read_some /
# close). :binstr on the recv side keeps bulk payloads binary-safe;
# write_bytes takes an explicit byte count so embedded NULs survive the
# send side too.
require "redis/sock"
require "redis/client"   # the error classes

class RedisTransport
  # read_timeout: seconds each read waits for the server, and
  # connect_timeout: seconds the connect waits, as redis-rb's options of
  # the same names; nil waits forever. A connect that runs out is a
  # Redis::CannotConnectError, as in redis-rb.
  def initialize(host, port, read_timeout = nil, connect_timeout = nil)
    @read_timeout = read_timeout.nil? ? -1.0 : read_timeout.to_f
    limit = connect_timeout.nil? ? -1.0 : connect_timeout.to_f
    @fd = RedisSock.sp_net_connect_timeout(host, port, limit)
    if @fd < 0
      if RedisSock.sp_net_take_connect_timeout == 1
        raise Redis::CannotConnectError.new("redis: no connection to " + host + ":" + port.to_s + " within " + limit.to_s + "s")
      end
      raise Redis::CannotConnectError.new("redis: cannot connect to " + host + ":" + port.to_s)
    end
  end

  # The raw socket fd — embedders with their own event loop park on
  # this (poll / Tep::Scheduler.io_wait) and then drain.
  def fd
    @fd
  end

  def write(data)
    RedisSock.sp_net_write_bytes(@fd, data, data.bytesize)
  end

  # A read that runs out of read_timeout raises Redis::TimeoutError and
  # closes the socket: a reply arriving later would be read as the answer
  # to the next command. redis-rb reconnects on the next command; here
  # the next command raises ConnectionError, so make a new client.
  def read_some(max)
    chunk = RedisSock.sp_net_recv_some_timeout(@fd, max, @read_timeout)
    if chunk.bytesize == 0 && RedisSock.sp_net_take_recv_timeout == 1
      close
      raise Redis::TimeoutError.new("redis: no reply within " + @read_timeout.to_s + "s")
    end
    chunk
  end

  def close
    if @fd < 0
      return 0
    end
    rc = RedisSock.sp_net_close(@fd)
    @fd = -1
    rc
  end
end
