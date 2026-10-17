# redis-rb's error classes, under the same names, so code can rescue a
# lost connection apart from a command the server refused, and the same
# rescue works against either client.
#
# Loaded from the bottom of redis/client.rb, after RedisClientCore
# exists: Redis's superclass has to be known where the class first
# opens. The messages are unchanged from v0.1 ("redis: ...").
class Redis < RedisClientCore
  # Base error for all errors this client raises.
  class BaseError < StandardError
  end

  # The server answered with an error reply.
  class CommandError < BaseError
  end

  # NOPERM: the ACL user may not run the command.
  class PermissionError < CommandError
  end

  # WRONGTYPE: the key holds another type.
  class WrongTypeError < CommandError
  end

  # OOM: the server is over maxmemory.
  class OutOfMemoryError < CommandError
  end

  # Base error for connection errors.
  class BaseConnectionError < BaseError
  end

  # No connection could be made.
  class CannotConnectError < BaseConnectionError
  end

  # The connection was lost.
  class ConnectionError < BaseConnectionError
  end

  # A command error reply, as the redis-rb class its prefix names.
  def self.command_error(message)
    if message.start_with?("WRONGTYPE")
      return WrongTypeError.new("redis: " + message)
    end
    if message.start_with?("NOPERM")
      return PermissionError.new("redis: " + message)
    end
    if message.start_with?("OOM")
      return OutOfMemoryError.new("redis: " + message)
    end
    CommandError.new("redis: " + message)
  end
end
