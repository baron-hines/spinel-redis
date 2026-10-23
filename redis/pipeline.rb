# Pipelining, in redis-rb's block form:
#
#   r.pipelined do |p|
#     p.sadd("queues", "default")
#     p.lpush("queue:default", job)
#   end                               # => [1, 1]
#
# The block queues commands on a RedisPipeline; nothing is sent until it
# returns. Then every command goes out in one write, and every reply is
# read, so the connection stays in step even when one of them is an
# error. The first error reply is raised after that, as redis-rb raises
# it. A pipeline is not a transaction: each command runs on its own.
#
# The recorder has its own typed method per command, like the client,
# and only the commands a caller has needed so far.
class RedisPipeline
  # How a reply is read back: the same coercions as the client's.
  INT = 0
  BOOL = 1
  STR = 2

  def initialize
    @argvs = [["PING"]]
    @argvs.clear
    @kinds = [INT]
    @kinds.clear
  end

  def argvs
    @argvs
  end

  def kinds
    @kinds
  end

  def queue(argv, kind)
    @argvs.push(argv)
    @kinds.push(kind)
    nil
  end

  def set(key, value)
    queue(["SET", key.to_s, value.to_s], STR)
  end

  def del(key)
    queue(["DEL", key.to_s], INT)
  end

  def incrby(key, n)
    queue(["INCRBY", key.to_s, n.to_s], INT)
  end

  def expire(key, seconds)
    queue(["EXPIRE", key.to_s, seconds.to_s], BOOL)
  end

  def lpush(key, value)
    queue(["LPUSH", key.to_s, value.to_s], INT)
  end

  def rpush(key, value)
    queue(RedisClientCore.push_argv("RPUSH", key, value), INT)
  end

  def sadd(key, member)
    queue(["SADD", key.to_s, member.to_s], INT)
  end

  def hset(key, field, value)
    queue(["HSET", key.to_s, field.to_s, value.to_s], INT)
  end

  def zadd(key, score, member)
    queue(["ZADD", key.to_s, score.to_s, member.to_s], BOOL)
  end

  def zremrangebyscore(key, min, max)
    queue(["ZREMRANGEBYSCORE", key.to_s, min.to_s, max.to_s], INT)
  end

  def zremrangebyrank(key, start, stop)
    queue(["ZREMRANGEBYRANK", key.to_s, start.to_s, stop.to_s], INT)
  end
end
