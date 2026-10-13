# Live flow for Redis.new(url:), AUTH, SELECT and the error classes,
# against a test-owned redis-server started with --requirepass and one
# ACL user. Spinel-only (the committed .expected keeps spin test from
# running it under CRuby); oracle/auth_live.rb replays it through the
# real redis-rb gem.
#
# Requires redis-server + redis-cli on PATH.
require "redis"

module AuthShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

AUTH_PORT = 16430
AUTH_URL = "redis://:s3cret@127.0.0.1:16430"

def auth_sh(cmd)
  AuthShell.sp_net_shell_capture(cmd, 256)
end

def auth_await
  attempts = 0
  while true
    begin
      c = Redis.new(url: AUTH_URL)   # assign-then-return: matz/spinel#1775
      c.ping
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "auth test: redis-server did not come up on " + AUTH_PORT.to_s
      end
      auth_sh("sleep 0.1")
    end
  end
end

def kind_of_error(e)
  if e.is_a?(Redis::CannotConnectError)
    return "CannotConnectError"
  end
  if e.is_a?(Redis::BaseConnectionError)
    return "BaseConnectionError"
  end
  if e.is_a?(Redis::CommandError)
    return "CommandError"
  end
  "other"
end

auth_sh("redis-server --port 16430 --requirepass s3cret --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-auth.pid --logfile /tmp/spinel-redis-auth.log")
admin = auth_await
admin.flushdb
puts "ping_url     " + admin.ping

# no password: the server refuses commands
begin
  anon = Redis.new("127.0.0.1", AUTH_PORT)
  anon.ping
  puts "noauth       accepted?!"
rescue => e
  puts "noauth       " + kind_of_error(e) + " " + e.message.include?("NOAUTH").to_s
end

# wrong password
begin
  bad = Redis.new(url: "redis://:wrong@127.0.0.1:16430")
  bad.ping
  puts "wrongpass    accepted?!"
rescue => e
  puts "wrongpass    " + kind_of_error(e) + " " + e.message.include?("WRONGPASS").to_s
end

# the password from a keyword, overriding the URL's
kw = Redis.new(url: "redis://:wrong@127.0.0.1:16430", password: "s3cret")
puts "pw_keyword   " + kw.ping
kw.close

# SELECT from the URL's path, and db: overriding it
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

# an ACL user: AUTH username password
auth_sh("redis-cli -p 16430 -a s3cret --no-auth-warning ACL SETUSER app on '>apppw' '~*' '+@all' >/dev/null")
app = Redis.new(url: "redis://app:apppw@127.0.0.1:16430/0")
puts "acl_user     " + app.ping
app.close

# the server goes away: a lost connection, not a command error
lost = Redis.new(url: AUTH_URL)
puts "before_kill  " + lost.ping
auth_sh("redis-cli -p 16430 -a s3cret --no-auth-warning shutdown nosave 2>/dev/null")
auth_sh("sleep 0.3")
begin
  lost.ping
  puts "after_kill   pinged?!"
rescue => e
  puts "after_kill   " + e.is_a?(Redis::BaseConnectionError).to_s
end

# nothing listening: CannotConnectError
begin
  none = Redis.new(url: AUTH_URL)
  none.ping
  puts "no_server    connected?!"
rescue => e
  puts "no_server    " + kind_of_error(e)
end
puts "done"
