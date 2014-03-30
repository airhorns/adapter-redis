require 'adapter'
require 'redis'

module Adapter
  module Redis
    def read(key, options = nil)
      decode(client.get(key))
    end

    def read_multiple(keys, options = nil)
      client.mapped_mget(*keys).reduce({}) do |result, (key, value)|
        result[key] = decode(value)
        result
      end
    end

    def write(key, value, options = nil)
      client.set(key, encode(value))
    end

    def delete(key, options = nil)
      client.del(key)
    end

    def clear(options = nil)
      client.flushdb
    end

    # Pretty much stolen from redis objects
    # http://github.com/nateware/redis-objects/blob/master/lib/redis/lock.rb
    def lock(name, options={}, &block)
      key           = name.to_s
      start         = Time.now
      acquired_lock = false
      expiration    = nil
      expires_in    = options.fetch(:expiration, 1)
      timeout       = options.fetch(:timeout, 5)

      while (Time.now - start) < timeout
        expiration    = generate_expiration(expires_in)
        acquired_lock = client.setnx(key, expiration)
        break if acquired_lock

        old_expiration = client.get(key).to_f

        if old_expiration < Time.now.to_f
          expiration     = generate_expiration(expires_in)
          old_expiration = client.getset(key, expiration).to_f

          if old_expiration < Time.now.to_f
            acquired_lock = true
            break
          end
        end

        sleep 0.1
      end

      raise(LockTimeout.new(name, timeout)) unless acquired_lock

      begin
        yield
      ensure
        client.del(key) if expiration > Time.now.to_f
      end
    end

    # Defaults expiration to 1
    def generate_expiration(expiration)
      (Time.now + (expiration || 1).to_f).to_f
    end

    private

    def decode(string_or_nil)
      return nil unless string_or_nil
      if options[:serializer]
        begin
          return options[:serializer].load(string_or_nil)
        rescue => e
          raise unless options[:deserializer_exception] && e.class == options[:deserializer_exception]
        end
      end
      Marshal.load(string_or_nil)
    end

    def encode(hash)
      serializer = options[:serializer] || Marshal
      serializer.dump(hash)
    end
  end
end

Adapter.define(:redis, Adapter::Redis)
