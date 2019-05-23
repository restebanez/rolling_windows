require_relative './time_buckets'

class RollingWindow
  attr_reader :redis, :time_buckets, :time_now

  def initialize(redis, time_span_windows)
    @redis = redis
    @time_buckets = TimeBuckets.new(time_span_windows)
  end

  def query_since(time_since:, user_id: 'all', record_types: ['d','b','f','rb'] )
    buckets_to_query = get_buckets_to_query(time_since, user_id, record_types)
    starting = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    ending = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    response = redis.mget(buckets_to_query)
    {
        record_types: record_types,
        sum: response.map(&:to_i).sum,
        queried_seconds_range: (Time.now - time_since).to_i,
        queried_buckets_count: response.size,
        matched_queried_buckets_count: response.compact.size,
        redis_query_time: ending - starting,
        queried_buckets: buckets_to_query
    }
  end

  def incr_windows_counter(user_id: , record_type: ,time: Time.now, adjust_expiration: true)
    init_stats = { creation_time: time, keys: [], windows: [], sum: 0, all_users_sum: 0}
    time_buckets.get_buckets_at(time).each_with_object(init_stats) do |window, stats|
      expiration = window.fetch(:expiration)
      expiration -= (Time.now - time) if adjust_expiration
      raise(ArgumentError, "We can't set an already expired record: #{expiration}") if expiration <= 0
      stats[:windows] << window.merge(expiration: expiration)
      user_result = incr_window_counter(user_id: user_id, record_type: record_type, window: window)
      stats[:keys] << user_result
      stats[:sum] += user_result[:value]
      all_users_result = incr_window_counter(user_id: 'all', record_type: record_type, window: window)
      stats[:all_users_sum] += all_users_result[:value]
    end
  end

  private

  def incr_window_counter(user_id: , record_type:, window: )
    user_key_name = redis_user_key_name(window, user_id, record_type)
    {
        name: user_key_name,
        value: redis_incr(user_key_name, window.fetch(:expiration).to_i),
        ex: window.fetch(:expiration).to_i
    }
  end

  def get_buckets_to_query(time_since, user_id, record_types)
    time_buckets.find_since(time_since: time_since).inject([]) do |buckets, window|
      bucket_with_types = record_types.map { |record| redis_user_key_name(window, user_id, record) }
      buckets.concat(bucket_with_types)
    end
  end

  def redis_incr(key_name, expiration)
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
