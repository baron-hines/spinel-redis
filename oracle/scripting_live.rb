# Oracle twin of test/scripting_live_test.rb (real redis-rb gem):
# script(:load), positional evalsha(sha, keys, argv), and
# Redis::NoScriptError after SCRIPT FLUSH.
require "redis"

ZPOPBYSCORE = <<~LUA
  local key, now = KEYS[1], ARGV[1]
  local jobs = redis.call("zrange", key, "-inf", now, "byscore", "limit", 0, 1)
  if jobs[1] then
    redis.call("zrem", key, jobs[1])
    return jobs[1]
  end
LUA

r = Redis.new(url: "redis://127.0.0.1:#{Integer(ARGV[0] || "6379")}/1")
r.flushdb

sha = r.script(:load, ZPOPBYSCORE)
puts "sha          " + sha

r.zadd("retry", 200.5, "due-2")
r.zadd("retry", 100.25, "due-1")
r.zadd("retry", 9000000000.0, "later")
3.times do |i|
  got = r.evalsha(sha, ["retry"], ["1791599558.5"])
  puts "pop_" + i.to_s + "        " + (got.nil? ? "nil" : got)
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
puts "done"
