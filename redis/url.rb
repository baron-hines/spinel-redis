# redis:// URLs for Redis.new(url:), read the way redis-rb 5 reads them
# (redis-client's URLConfig):
#
#   redis://[[username]:password@]host[:port][/db][?db=N]
#
# - A user with no password is the password (redis://secret@host).
# - The password is percent-decoded, as decode_www_form_component does
#   ("+" is a space); the username is used as written.
# - The db comes from the path, else from a db= query parameter.
# - [::1] brackets are stripped from an IPv6 host.
# - The host defaults to localhost and the port to 6379.
#
# rediss:// and unix:// raise ArgumentError: sp_net has no TLS
# (matz/spinel#1054) and no unix sockets. Missing parts are "" or 0, not
# nil, so every field keeps one type.
class RedisURL
  def initialize(host, port, username, password, db)
    @host = host
    @port = port
    @username = username
    @password = password
    @db = db
  end

  def host
    @host
  end

  def port
    @port
  end

  # "" when the URL names no user.
  def username
    @username
  end

  # "" when the URL carries no password.
  def password
    @password
  end

  def db
    @db
  end

  def self.parse(url)
    if url.start_with?("rediss://")
      raise ArgumentError.new("redis: rediss:// (TLS) is not supported; sp_net has no TLS yet (matz/spinel#1054). Put a local TLS proxy such as stunnel in front and use redis://")
    end
    if url.start_with?("unix://") || url.start_with?("/")
      raise ArgumentError.new("redis: unix sockets are not supported; use redis://host:port")
    end
    if !url.start_with?("redis://")
      raise ArgumentError.new("redis: unknown URL scheme in " + RedisURL.redact(url))
    end
    rest = url[8, url.length - 8]

    query = ""
    q = rest.index("?")
    if !q.nil?
      query = rest[q + 1, rest.length - q - 1]
      rest = rest[0, q]
    end

    path = ""
    s = rest.index("/")
    if !s.nil?
      path = rest[s + 1, rest.length - s - 1]
      rest = rest[0, s]
    end

    username = ""
    password = ""
    at = rest.rindex("@")
    if !at.nil?
      userinfo = rest[0, at]
      rest = rest[at + 1, rest.length - at - 1]
      colon = userinfo.index(":")
      if colon.nil?
        password = RedisURL.decode(userinfo)
      else
        username = userinfo[0, colon]
        password = RedisURL.decode(userinfo[colon + 1, userinfo.length - colon - 1])
      end
    end

    host = rest
    port = 6379
    if rest.start_with?("[")
      close = rest.index("]")
      if close.nil?
        raise ArgumentError.new("redis: bad IPv6 host in " + RedisURL.redact(url))
      end
      host = rest[1, close - 1]
      after = rest[close + 1, rest.length - close - 1]
      if after.start_with?(":")
        port = RedisURL.number(after[1, after.length - 1], url)
      end
    else
      colon = rest.rindex(":")
      if !colon.nil?
        host = rest[0, colon]
        port = RedisURL.number(rest[colon + 1, rest.length - colon - 1], url)
      end
    end
    if host == ""
      host = "localhost"
    end

    db = 0
    if path != ""
      db = RedisURL.number(path, url)
    else
      query.split("&").each do |pair|
        if pair.start_with?("db=") && pair.length > 3
          db = RedisURL.number(pair[3, pair.length - 3], url)
        end
      end
    end

    RedisURL.new(host, port, username, password, db)
  end

  # A decimal port or db number; anything else is an ArgumentError.
  def self.number(text, url)
    if text == ""
      raise ArgumentError.new("redis: missing number in " + RedisURL.redact(url))
    end
    i = 0
    while i < text.length
      c = text.getbyte(i)
      if c < 48 || c > 57
        raise ArgumentError.new("redis: bad number " + text.inspect + " in " + RedisURL.redact(url))
      end
      i = i + 1
    end
    text.to_i
  end

  # Percent-decoding, with "+" as a space (decode_www_form_component).
  def self.decode(text)
    bytes = []
    i = 0
    n = text.bytesize
    while i < n
      c = text.getbyte(i)
      if c == 43
        bytes.push(32)
        i = i + 1
      elsif c == 37 && i + 2 < n && RedisURL.hex(text.getbyte(i + 1)) >= 0 && RedisURL.hex(text.getbyte(i + 2)) >= 0
        bytes.push(RedisURL.hex(text.getbyte(i + 1)) * 16 + RedisURL.hex(text.getbyte(i + 2)))
        i = i + 3
      else
        bytes.push(c)
        i = i + 1
      end
    end
    bytes.pack("C*").force_encoding("UTF-8")
  end

  def self.hex(c)
    if c >= 48 && c <= 57
      return c - 48
    end
    if c >= 65 && c <= 70
      return c - 55
    end
    if c >= 97 && c <= 102
      return c - 87
    end
    -1
  end

  # The URL with any password replaced by REDACTED, for messages and
  # logs, as Sidekiq's own scrub does.
  def self.redact(url)
    scheme = url.index("://")
    if scheme.nil?
      return url
    end
    start = scheme + 3
    tail = url[start, url.length - start]
    at = tail.rindex("@")
    slash = tail.index("/")
    if at.nil? || (!slash.nil? && slash < at)
      return url
    end
    userinfo = tail[0, at]
    colon = userinfo.index(":")
    shown = colon.nil? ? "REDACTED" : userinfo[0, colon] + ":REDACTED"
    url[0, start] + shown + tail[at, tail.length - at]
  end
end
