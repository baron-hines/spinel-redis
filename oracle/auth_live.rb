# Oracle twin of test/auth_live_test.rb (real redis-rb gem): the same
# flow, with its own --requirepass server on the same private port.
# redis-rb connects lazily, so each connect is followed by the command
# that would fail, as in the spinel flow.
require "redis"

AUTH_PORT = 16430
AUTH_URL = "redis://:s3cret@127.0.0.1:16430"

def auth_sh(cmd)
  system(cmd)
end

def auth_await
  50.times do
    begin
      c = Redis.new(url: AUTH_URL)
      c.ping
      return c
    rescue StandardError
      sleep 0.1
    end
  end
  raise "auth oracle: redis-server did not come up on #{AUTH_PORT}"
end

def kind_of_error(e)
  return "CannotConnectError" if e.is_a?(Redis::CannotConnectError)
  return "BaseConnectionError" if e.is_a?(Redis::BaseConnectionError)
  return "CommandError" if e.is_a?(Redis::CommandError)
  "other"
end

auth_sh("redis-server --port 16430 --requirepass s3cret --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-auth-oracle.pid --logfile /tmp/spinel-redis-auth-oracle.log")
admin = auth_await
admin.flushdb
puts "ping_url     " + admin.ping

begin
  anon = Redis.new(host: "127.0.0.1", port: AUTH_PORT)
  anon.ping
  puts "noauth       accepted?!"
rescue => e
  puts "noauth       " + kind_of_error(e) + " " + e.message.include?("NOAUTH").to_s
end

begin
  bad = Redis.new(url: "redis://:wrong@127.0.0.1:16430")
  bad.ping
  puts "wrongpass    accepted?!"
rescue => e
  puts "wrongpass    " + kind_of_error(e) + " " + e.message.include?("WRONGPASS").to_s
end

kw = Redis.new(url: "redis://:wrong@127.0.0.1:16430", password: "s3cret")
puts "pw_keyword   " + kw.ping
kw.close

db3 = Redis.new(url: AUTH_URL + "/3")
db3.flushdb
puts "set_db3      " + db3.set("auth:k", "in-three")
db0 = Redis.new(url: AUTH_URL + "/0")
g0 = db0.get("auth:k")
puts "db0_missing  " + g0.nil?.to_s
db5 = Redis.new(url: AUTH_URL + "/5", db: 3)
g3 = db5.get("auth:k")
puts "db_keyword   " + g3.to_s
db3.del("auth:k")
db3.close
db0.close
db5.close

auth_sh("redis-cli -p 16430 -a s3cret --no-auth-warning ACL SETUSER app on '>apppw' '~*' '+@all' >/dev/null")
app = Redis.new(url: "redis://app:apppw@127.0.0.1:16430/0")
puts "acl_user     " + app.ping
app.close

lost = Redis.new(url: AUTH_URL)
puts "before_kill  " + lost.ping
auth_sh("redis-cli -p 16430 -a s3cret --no-auth-warning shutdown nosave 2>/dev/null")
sleep 0.3
begin
  lost.ping
  puts "after_kill   pinged?!"
rescue => e
  puts "after_kill   " + e.is_a?(Redis::BaseConnectionError).to_s
end

begin
  none = Redis.new(url: AUTH_URL)
  none.ping
  puts "no_server    connected?!"
rescue => e
  puts "no_server    " + kind_of_error(e)
end
puts "done"
