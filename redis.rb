# redis — a pure spinel-Ruby Redis client (RESP2) over sp_net sockets.
#
# The require string mirrors redis-rb so transpiled `require "redis"`
# resolves here; the command surface mirrors redis-rb's contracts
# (nil for missing keys, booleans for EXISTS/EXPIRE/SISMEMBER, Hash for
# HGETALL, redis-rb's error classes). See README for the exclusion
# ledger (Sentinel, cluster, TLS-only endpoints, RESP3).
require "redis/resp"
require "redis/client"
require "redis/pubsub"
require "redis/sock"
require "redis/connection"
require "redis/url"

class Redis < RedisClientCore
  # Redis.new                                  -> 127.0.0.1:6379
  # Redis.new("10.0.0.5", 6380)                -> positional, as in v0.1
  # Redis.new(url: "redis://:pw@host:6379/2")  -> redis-rb's form
  #
  # read_timeout: is how many seconds each read waits for a reply before
  # raising Redis::TimeoutError, and connect_timeout: how long the
  # connect waits before raising Redis::CannotConnectError (nil, the
  # default for both, waits forever).
  # url: wins over host and port. password:, username: and db: override
  # what the URL says, as redis-rb's explicit options do. After connect
  # it sends AUTH when there is a password and SELECT when db isn't 0.
  def initialize(host = "127.0.0.1", port = 6379, url: nil, username: nil, password: nil, db: 0, read_timeout: nil, connect_timeout: nil)
    target = Redis.target(host, port, url, username, password, db)
    super(RedisTransport.new(target.host, target.port, read_timeout, connect_timeout))
    handshake(target)
  end

  # AUTH and SELECT right after connect. A refusal here is a failure to
  # connect, as redis-rb reports it (a wrong password is a
  # CannotConnectError there, not a CommandError).
  def handshake(target)
    begin
      if target.password != ""
        if target.username != ""
          auth(target.username, target.password)
        else
          auth(target.password)
        end
      end
      if target.db != 0
        select(target.db)
      end
    rescue Redis::CommandError => e
      close
      raise Redis::CannotConnectError.new(e.message)
    end
    nil
  end

  # A subscribed connection is a dedicated connection (RESP2), so pubsub
  # gets its own transport rather than sharing this client's. It
  # authenticates before entering subscribe mode; channels aren't
  # per-db, so there is no SELECT.
  def self.pubsub(host = "127.0.0.1", port = 6379, url: nil, username: nil, password: nil)
    target = Redis.target(host, port, url, username, password, 0)
    t = RedisTransport.new(target.host, target.port)
    if target.password != ""
      login = RedisClientCore.new(t)
      begin
        if target.username != ""
          login.auth(target.username, target.password)
        else
          login.auth(target.password)
        end
      rescue Redis::CommandError => e
        t.close
        raise Redis::CannotConnectError.new(e.message)
      end
    end
    RedisPubSub.new(t)
  end

  # Where to connect and how to log in, from the constructor's arguments.
  def self.target(host, port, url, username, password, db)
    base = url.nil? ? RedisURL.new(host, port, "", "", 0) : RedisURL.parse(url)
    RedisURL.new(base.host, base.port,
      username.nil? ? base.username : username,
      password.nil? ? base.password : password,
      db != 0 ? db : base.db)
  end
end
