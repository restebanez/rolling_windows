require_relative './time_buckets'

class RollingWindow
  attr_reader :redis, :time_buckets, :time_now
  NEVER_EXPIRE_VALUE = -1


  def initialize(redis, time_span_windows = TimeBuckets::DEFAULT_TIME_SPAN_WINDOWS)
    @redis = redis
    @time_buckets = TimeBuckets.new(time_span_windows)
  end

  def query_since(time_since:, user_id: 'all', pmta_record_types: ['d','b','f','rb'] )
    all_redis_keys = get_redis_keys(time_since, user_id, pmta_record_types)
    buckets_with_type = all_redis_keys.fetch(:existing)
    buckets_to_query = buckets_with_type.map { |a| a.fetch(:redis_key_name) }
    records_types = buckets_with_type.map { |a| a.fetch(:pmta_record_type) }
    starting = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    ending = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    redis_response = redis.mget(buckets_to_query)
    {
        pmta_record_types: pmta_record_types,
        sum: redis_response.map(&:to_i).sum,
        queried_seconds_range: (Time.now - time_since).to_i,
        queried_buckets_count: redis_response.size,
        matched_queried_buckets_count: redis_response.compact.size,
        skipped_expired_buckets: all_redis_keys.fetch(:expired, []).map { |a| a.fetch(:redis_key_name) },
        redis_query_time: ending - starting,
        stats_per_pmta_record_type: generate_stats(redis_response, records_types),
        queried_buckets: buckets_to_query
    }
  end

  def incr_windows_counter(user_id: , pmta_record_type: ,time: Time.now, never_expire: false)
    init_stats = { creation_time: time, keys: [], windows: [], sum: 0, all_users_sum: 0}
    time_buckets.get_buckets_at(time, never_expire).each_with_object(init_stats) do |window, stats|
      validate_expiration(window.fetch(:expire_at)) unless never_expire
      user_result = incr_window_counter(user_id: user_id, pmta_record_type: pmta_record_type, window: window)
      stats[:windows] << window
      stats[:keys] << user_result
      stats[:sum] += user_result[:value]
      all_users_result = incr_window_counter(user_id: 'all', pmta_record_type: pmta_record_type, window: window)
      stats[:all_users_sum] += all_users_result[:value]
    end
  end

  private

  def validate_expiration(expire_at)
    error_msg = "We can't set an already expired record: #{expire_at}"
    raise(ArgumentError, error_msg) if Time.now > expire_at
  end

  def generate_stats(response, records_types)
    response.zip(records_types).select{|response,_type| response}.each_with_object({}) do |(response, pmta_record_type), stats|
      stats[pmta_record_type] ||= 0
      stats[pmta_record_type] += response.to_i
    end
  end

  def incr_window_counter(user_id:, pmta_record_type:, window: )
    user_key_name = redis_user_key_name(window, user_id, pmta_record_type)
    expiration = if window.fetch(:expire_at) == NEVER_EXPIRE_VALUE
                   NEVER_EXPIRE_VALUE
                 else
                   window.fetch(:expire_at) - Time.now
                 end

    {
        name: user_key_name,
        value: redis_incr(user_key_name, expiration.to_i),
        ex: expiration.to_i
    }
  end

  def get_redis_keys(time_since, user_id, pmta_record_types)
    init = { existing: [], expired: [] }
    time_buckets.find_since(time_since: time_since).each_with_object(init) do |window, redis_keys|
      #puts "get_redis_keys #redis_keys #{redis_keys.fetch(:existing,[]).size}"
      key_category = window.fetch(:expire_at) > Time.now ? :existing : :expired
      redis_keys[key_category].concat(expand_redis_key_names(pmta_record_types, window, user_id))
    end
  end

  def expand_redis_key_names(pmta_record_types, window, user_id)
    pmta_record_types.map do |pmta_record_type|
      {
          redis_key_name: redis_user_key_name(window, user_id, pmta_record_type),
          pmta_record_type: pmta_record_type
      }
    end
  end

  def redis_incr(key_name, expiration)
    result = redis.eval(lua_set_or_inc(expiration), :keys => [key_name])
    get_time_counter(result)
  end

  def redis_user_key_name(window, user_id, pmta_record_type)
    "at:#{window.fetch(:window_starts).to_i}:for:#{window.fetch(:span)}:u:#{user_id}:#{pmta_record_type}"
  end

  def get_time_counter(result)
    result == 'OK' ? 1 : result.to_i
  end

  def lua_set_or_inc(seconds_to_expire)
    if seconds_to_expire.to_i > 0
      "return redis.call('set',KEYS[1], 1,'EX', #{seconds_to_expire}, 'NX') or redis.call('incr', KEYS[1])"
    else
      "return redis.call('incr', KEYS[1])"
    end
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

  def redis_field_name(pmta_record_type: ,user_id:)
    "#{user_id}:#{pmta_record_type}"
  end

  def lua_hincr
    "local v = redis.call('HINCRBY', KEYS[1], KEYS[2], 1) if v == 1 then redis.call('EXPIRE', KEYS[1], 100) end return v"
  end
=end
