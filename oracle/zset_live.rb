# Oracle twin of test/zset_live_test.rb (real redis-rb gem): Boolean
# from single-pair zadd/zrem, Float from zscore, the same db 1 via url:.
require "redis"

r = Redis.new(url: "redis://127.0.0.1:#{Integer(ARGV[0] || "6379")}/1")
r.flushdb

scores = [1791599858.8335917, 1791599558.332 + 300.5, 1791599558.0, 123456789.12345679, 0.1 + 0.2, 1791599558.9999998]
scores.each_with_index do |s, i|
  puts "zadd_new     " + r.zadd("schedule", s, "job-" + i.to_s).to_s
end
puts "zadd_again   " + r.zadd("schedule", 42, "job-0").to_s
puts "zcard        " + r.zcard("schedule").to_s

scores.each_index do |i|
  got = r.zscore("schedule", "job-" + i.to_s)
  want = i == 0 ? 42.0 : scores[i]
  puts "zscore_" + i.to_s + "     " + (got == want).to_s + " " + got.to_s
end
m = r.zscore("schedule", "missing")
puts "zscore_nil   " + m.nil?.to_s

puts "zrange       " + r.zrange("schedule", 0, -1).join(",")
puts "zrange_top2  " + r.zrange("schedule", -2, -1).join(",")
puts "zrem         " + r.zrem("schedule", "job-4").to_s
puts "zrem_again   " + r.zrem("schedule", "job-4").to_s
puts "zcard_after  " + r.zcard("schedule").to_s

r.lpush("schedule:list", "x")
begin
  r.zadd("schedule:list", 1, "m")
  puts "wrongtype    added?!"
rescue Redis::WrongTypeError => e
  puts "wrongtype    " + e.is_a?(Redis::CommandError).to_s
end

r.close
puts "done"
