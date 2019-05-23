require_relative './time_buckets'

class RollingWindow
  attr_reader :redis, :time_buckets, :time_now

  def initialize(redis, time_span_windows)
    @redis = redis
    @time_buckets = TimeBuckets.new(time_span_windows)
  end

  def incr_windows_counter(user_id: , record_type: ,time: Time.now, adjust_expiration: true)
    time_buckets.get_buckets_at(time).each_with_object({ creation_time: time, keys: [], windows: [], sum: 0}) do |window, stats|
      key_name = redis_user_key_name(window, user_id, record_type)
      expiration = window.fetch(:expiration)
      expiration -= (Time.now - time) if adjust_expiration
      raise(ArgumentError, "We can't set an already expire record: #{expiration}") if expiration <= 0
      value = incr(key_name, expiration.to_i)
      stats[:keys] << { name: key_name, value: value, ex: expiration.to_i}
      stats[:sum] += value
      stats[:windows] << window.merge(expiration: expiration)
    end
  end

  def incr(key_name, expiration)
    result = redis.eval(lua_set_or_inc(expiration), :keys => [key_name])
    get_time_counter(result)
  end

  def redis_user_key_name(window, user_id, record_type)
    "at:#{window.fetch(:window_starts).to_i}:for:#{window.fetch(:span)}:u:#{user_id}:#{record_type}"
  end

  def get_time_counter(result)
    result == 'OK' ? 1 : result.to_i
  end

  def lua_set_or_inc(seconds_to_expire)
    "return redis.call('set',KEYS[1], 1,'EX', #{seconds_to_expire}, 'NX') or redis.call('incr', KEYS[1])"
  end
end

# using hashes
=begin
  def hincr(window, *params)
    puts window.fetch(:expiration).to_i
    result = redis.eval(lua_hincr,
                        keys: [redis_key_name(window),
                               redis_field_name(*params)],
                        argv:[window.fetch(:expiration).to_i])
    puts result
  end

  def redis_key_name(window)
    "at:#{window.fetch(:window_starts).to_i}:for:#{window.fetch(:span)}"
  end

  def redis_field_name(record_type: ,user_id:)
    "#{user_id}:#{record_type}"
  end

  def lua_hincr
    "local v = redis.call('HINCRBY', KEYS[1], KEYS[2], 1) if v == 1 then redis.call('EXPIRE', KEYS[1], 100) end return v"
  end
=end
