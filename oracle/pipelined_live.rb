# Oracle twin of test/pipelined_live_test.rb (real redis-rb gem), with
# its own --requirepass server on the same private port.
require "redis"

PIPE_URL = "redis://:pipepw@127.0.0.1:16432/2"

def same_bytes(a, b)
  a.b == b.b
end

system("redis-server --port 16432 --requirepass pipepw --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-pipe-oracle.pid --logfile /tmp/spinel-redis-pipe-oracle.log")
r = nil
50.times do
  begin
    r = Redis.new(url: PIPE_URL)
    r.ping
    break
  rescue StandardError
    r = nil
    sleep 0.1
  end
end
raise "pipelined oracle: redis-server did not come up on 16432" if r.nil?
r.flushdb

job = "{\"class\":\"Sidekiq::ActiveJob::Wrapper\",\"queue\":\"default\",\"args\":[\"é ✓\"]}"
res = r.pipelined do |p|
  p.sadd("queues", "default")
  p.lpush("queue:default", job)
end
puts "push_1       " + res.length.to_s + " " + res[0].to_s + " " + res[1].to_s
res = r.pipelined do |p|
  p.sadd("queues", "default")
  p.lpush("queue:default", job)
end
puts "push_2       " + res[0].to_s + " " + res[1].to_s
puts "llen         " + r.llen("queue:default").to_s
puts "queues       " + r.smembers("queues").sort.join(",")
got = r.lrange("queue:default", 0, 0)
puts "job_intact   " + same_bytes(got[0], job).to_s

at = 1791599858.8335917
res = r.pipelined do |p|
  p.zadd("schedule", at, job)
end
puts "zadd_piped   " + res[0].to_s
puts "zscore       " + (r.zscore("schedule", job) == at).to_s

res = r.pipelined do |p|
  p.set("stat:processed", "0")
  p.incrby("stat:processed", 5)
  p.expire("stat:processed", 60)
  p.hset("worker:1", "busy", "0")
  p.del("stat:processed")
end
puts "mixed        " + res[0].to_s + " " + res[1].to_s + " " + res[2].to_s + " " + res[3].to_s + " " + res[4].to_s

begin
  r.pipelined do |p|
    p.rpush("after:1", "a")
    p.lpush("queues", "not-a-list")
    p.rpush("after:2", "b")
  end
  puts "pipe_error   none?!"
rescue Redis::CommandError => e
  puts "pipe_error   " + e.is_a?(Redis::WrongTypeError).to_s
end
puts "rest_ran     " + r.llen("after:1").to_s + " " + r.llen("after:2").to_s
puts "in_sync      " + r.ping

system("redis-cli -p 16432 -a pipepw --no-auth-warning shutdown nosave 2>/dev/null")
sleep 0.3
begin
  r.pipelined do |p|
    p.sadd("queues", "default")
    p.lpush("queue:default", job)
  end
  puts "after_kill   pushed?!"
rescue Redis::CommandError
  puts "after_kill   command error?!"
rescue Redis::BaseConnectionError
  puts "after_kill   connection error"
end
puts "done"
