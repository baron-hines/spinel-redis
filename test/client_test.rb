# Client-core conformance against a scripted transport — dual-runtime (no
# snapshot): the command bytes written and the reply coercions must match
# CRuby exactly. Wire bytes come straight from the RESP2 spec; the reply
# values mirror redis-rb's contracts (nil for missing keys, bool for
# EXISTS/EXPIRE/SISMEMBER, Hash for HGETALL).
require "redis/resp"
require "redis/client"
require "redis/url"

# Serves canned reply bytes chunk-by-chunk and records every write.
class ScriptedTransport
  def initialize(chunks)
    @chunks = chunks
    @i = 0
    @written = ""
  end

  def write(data)
    @written = @written + data
    data.bytesize
  end

  def read_some(max)
    if @i >= @chunks.length
      return ""
    end
    c = @chunks[@i]
    @i = @i + 1
    c
  end

  def close
    0
  end

  def written
    @written
  end
end

# GET hit: command encoding + bulk coercion
t = ScriptedTransport.new(["$3\r\nbar\r\n"])
c = RedisClientCore.new(t)
v = c.get("foo")
puts "get_wire     " + (t.written == "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n").to_s
puts "get_value    " + (v == "bar").to_s

# GET miss: $-1 -> nil
t = ScriptedTransport.new(["$-1\r\n"])
c = RedisClientCore.new(t)
v = c.get("absent")
puts "get_nil      " + v.nil?.to_s

# SET: status reply
t = ScriptedTransport.new(["+OK\r\n"])
c = RedisClientCore.new(t)
puts "set_ok       " + (c.set("k", "v") == "OK").to_s

# INCR: integer reply; INCRBY stringifies its argument
t = ScriptedTransport.new([":6\r\n", ":16\r\n"])
c = RedisClientCore.new(t)
a = c.incr("n")
b = c.incrby("n", 10)
w = t.written
puts "incr_values  " + (a == 6 && b == 16).to_s
puts "incrby_wire  " + w.include?("$6\r\nINCRBY\r\n$1\r\nn\r\n$2\r\n10\r\n").to_s

# SETEX wire shape
t = ScriptedTransport.new(["+OK\r\n"])
c = RedisClientCore.new(t)
ok = c.setex("pres", 60, "1") == "OK"
w = t.written
puts "setex        " + (ok && w.include?("$5\r\nSETEX\r\n$4\r\npres\r\n$2\r\n60\r\n$1\r\n1\r\n")).to_s

# EXISTS -> bool via :1/:0
t = ScriptedTransport.new([":1\r\n", ":0\r\n"])
c = RedisClientCore.new(t)
a = c.exists?("yes")
b = c.exists?("no")
puts "exists_bool  " + (a == true && b == false).to_s

# KEYS -> Array<String>
t = ScriptedTransport.new(["*2\r\n$1\r\na\r\n$1\r\nb\r\n"])
c = RedisClientCore.new(t)
ks = c.keys("*")
puts "keys_array   " + (ks.length == 2 && ks[0] == "a" && ks[1] == "b").to_s

# LRANGE with numeric args on the wire
t = ScriptedTransport.new(["*2\r\n$1\r\nx\r\n$1\r\ny\r\n"])
c = RedisClientCore.new(t)
l = c.lrange("list", 0, -1)
w = t.written
puts "lrange_wire  " + w.include?("$2\r\n-1\r\n").to_s
puts "lrange_vals  " + (l[0] == "x" && l[1] == "y").to_s

# HGETALL flat pairs -> Hash
t = ScriptedTransport.new(["*4\r\n$1\r\nf\r\n$1\r\n1\r\n$1\r\ng\r\n$1\r\n2\r\n"])
c = RedisClientCore.new(t)
h = c.hgetall("h")
puts "hgetall_hash " + (h.length == 2 && h["f"] == "1" && h["g"] == "2").to_s

# a reply dribbling in across many reads still coerces whole
t = ScriptedTransport.new(["$1", "1\r", "\nhello world", "\r\n"])
c = RedisClientCore.new(t)
v = c.get("k")
puts "chunked_read " + (v == "hello world").to_s

# binary-safe round trip: value with NUL and CRLF inside
bin = "a" + [0].pack("C*") + "\r\nb"
t = ScriptedTransport.new(["$" + bin.bytesize.to_s + "\r\n" + bin + "\r\n"])
c = RedisClientCore.new(t)
v = c.get("bin")
got = false
if !v.nil?
  got = v.bytesize == 5
  got = got && v.getbyte(1) == 0
end
puts "binary_value " + got.to_s

# -ERR reply raises
t = ScriptedTransport.new(["-ERR unknown command 'FROB'\r\n"])
c = RedisClientCore.new(t)
raised = false
begin
  c.ping
rescue => e
  raised = e.message.include?("unknown command")
end
puts "error_raises " + raised.to_s

# closed connection mid-reply raises
t = ScriptedTransport.new(["$10\r\npart"])
c = RedisClientCore.new(t)
raised = false
begin
  c.get("k")
rescue => e
  raised = e.message.include?("connection lost")
end
puts "closed_raise " + raised.to_s

# -- error classes: redis-rb names ------------------------------------------

# an error reply is a CommandError, a lost connection a ConnectionError,
# and neither is mistaken for the other
t = ScriptedTransport.new(["-ERR unknown command 'FROB'\r\n"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.ping
rescue Redis::BaseConnectionError
  kind = "connection"
rescue Redis::CommandError => e
  kind = "command " + e.message
end
puts "err_class    " + kind

t = ScriptedTransport.new(["-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.incr("s")
rescue Redis::WrongTypeError => e
  kind = "wrongtype " + e.is_a?(Redis::CommandError).to_s + " " + e.is_a?(Redis::BaseError).to_s
end
puts "err_wrongty  " + kind

t = ScriptedTransport.new(["-NOPERM this user has no permissions\r\n", "-OOM command not allowed\r\n"])
c = RedisClientCore.new(t)
kinds = ""
begin
  c.ping
rescue Redis::PermissionError
  kinds = kinds + "noperm "
end
begin
  c.ping
rescue Redis::OutOfMemoryError
  kinds = kinds + "oom"
end
puts "err_prefixes " + kinds

t = ScriptedTransport.new(["$10\r\npart"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.get("k")
rescue Redis::CommandError
  kind = "command"
rescue Redis::ConnectionError => e
  kind = "connection " + e.is_a?(Redis::BaseConnectionError).to_s + " " + e.message
end
puts "err_lost     " + kind

# -- AUTH -------------------------------------------------------------------

t = ScriptedTransport.new(["+OK\r\n", "+OK\r\n"])
c = RedisClientCore.new(t)
a = c.auth("s3cret")
b = c.auth("app", "pw")
w = t.written
puts "auth_wire    " + (w == "*2\r\n$4\r\nAUTH\r\n$6\r\ns3cret\r\n*3\r\n$4\r\nAUTH\r\n$3\r\napp\r\n$2\r\npw\r\n").to_s
puts "auth_reply   " + (a == "OK" && b == "OK").to_s

# -- redis:// URLs ----------------------------------------------------------

def url_line(url)
  u = RedisURL.parse(url)
  u.host + "|" + u.port.to_s + "|" + u.username + "|" + u.password + "|" + u.db.to_s
end

puts "url_bare     " + url_line("redis://localhost")
puts "url_full     " + url_line("redis://:s3cret@cache.internal:6380/2")
puts "url_user     " + url_line("redis://app:p%40ss+word@10.0.0.5/7")
puts "url_useronly " + url_line("redis://onlysecret@host")
puts "url_empty    " + url_line("redis://")
puts "url_query    " + url_line("redis://host:6390?db=4")
puts "url_ipv6     " + url_line("redis://[::1]:6381/1")

def url_error(url)
  begin
    RedisURL.parse(url)
    "parsed"
  rescue ArgumentError => e
    e.message
  end
end

puts "url_tls      " + url_error("rediss://:pw@host:6380").include?("rediss:// (TLS) is not supported").to_s
puts "url_unix     " + url_error("unix:///tmp/redis.sock").include?("unix sockets").to_s
puts "url_scheme   " + url_error("http://:pw@host")
puts "url_badport  " + url_error("redis://host:63x9")
puts "redact       " + RedisURL.redact("redis://app:hunter2@host:6379/0")
puts "redact_pw    " + RedisURL.redact("redis://:hunter2@host")
puts "redact_none  " + RedisURL.redact("redis://host:6379/0")

# -- sorted sets ----------------------------------------------------------------

# ZADD writes the score with to_s (Float and Integer) and reads :1/:0 as
# a Boolean, as redis-rb's single-pair form does
t = ScriptedTransport.new([":1\r\n", ":0\r\n"])
c = RedisClientCore.new(t)
a = c.zadd("schedule", 1791599858.8335917, "job-a")
b = c.zadd("schedule", 5, "job-b")
w = t.written
puts "zadd_wire    " + (w == "*4\r\n$4\r\nZADD\r\n$8\r\nschedule\r\n$18\r\n1791599858.8335917\r\n$5\r\njob-a\r\n*4\r\n$4\r\nZADD\r\n$8\r\nschedule\r\n$1\r\n5\r\n$5\r\njob-b\r\n").to_s
puts "zadd_bool    " + (a == true && b == false).to_s

# ZSCORE: bulk -> Float, $-1 -> nil, inf spelled as Redis does
t = ScriptedTransport.new(["$18\r\n1791599858.8335917\r\n", "$-1\r\n", "$3\r\ninf\r\n", "$4\r\n-inf\r\n"])
c = RedisClientCore.new(t)
s1 = c.zscore("schedule", "job-a")
s2 = c.zscore("schedule", "nope")
s3 = c.zscore("schedule", "far")
s4 = c.zscore("schedule", "past")
puts "zscore_float " + (s1 == 1791599858.8335917).to_s
puts "zscore_nil   " + s2.nil?.to_s
puts "zscore_inf   " + (s3 == Float::INFINITY && s4 == -Float::INFINITY).to_s

# ZCARD, ZREM, ZRANGE
t = ScriptedTransport.new([":2\r\n", ":1\r\n", "*1\r\n$5\r\njob-b\r\n"])
c = RedisClientCore.new(t)
n = c.zcard("schedule")
r = c.zrem("schedule", "job-a")
m = c.zrange("schedule", 0, -1)
puts "zcard_zrem   " + (n == 2 && r == true).to_s
puts "zrange       " + (m.length == 1 && m[0] == "job-b").to_s
