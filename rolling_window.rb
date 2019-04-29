class RollingWindow
  attr_reader :redis

  STRFTIME = {
      second: '%S',
      minute: '%M',
      hour: '%H'
  }

  EXPIRATION = {
      second: 60,
      minute: 60*60,
      hour: 60*60*24
  }

  def initialize(redis)
    @redis = redis
  end

  def register(user_id)
    time_now = Time.now
    [:second, :minute, :hour].each_with_object({}) do |precision, result|
      result["current_#{precision}".to_sym] = extract_time(time_now, precision)
      result["current_user_#{precision}".to_sym] = user_redis_key(user_id, time_now, precision)
      result["counter_#{precision}".to_sym] = incr(user_id, time_now, precision)
    end
  end

  def sum_last_x_seconds(user_id, seconds_back, current_second=Time.now.strftime('%S').to_i)
    seconds_range(finish_second: current_second, seconds_back: seconds_back ).each_with_object({total: 0, user_second_keys: []}) do |second, hash|
      key = "user:#{user_id}:second:#{second}"
      value = $redis_store_obj.get(key).to_i
      puts "#{key} #{value}" if value > 0
      hash[:user_second_keys] << key if value > 0
      hash[:user_second_keys].uniq!
      hash[:total] += value
    end
  end

  def sum_seconds_range(user_id, start, finish)
    last_second = get_last_second(start: start, finish: finish)
    sum_last_x_seconds(user_id, last_second, finish)
  end

  private

  def incr(user_id, time_now, time_precision)
    result = redis.eval(lua_set_or_inc(EXPIRATION.fetch(time_precision)), :keys => [user_redis_key(user_id, time_now, time_precision)])
    get_time_counter(result)
  end

  def user_redis_key(user_id, time_now, time_precision)
    "user:#{user_id}:#{time_precision}:#{extract_time(time_now, time_precision)}"
  end

  def extract_time(time_now, precision)
    time_now.strftime(STRFTIME.fetch(precision)).to_i
  end

  def get_last_second(start:, finish:)
    finish += 60 if finish < start
    finish - start
  end

  def seconds_range(finish_second:, seconds_back: )
    (0..seconds_back).map {|s| (finish_second-=1) + 1 }.sort.map {|i| i < 0 ? i + 60 : i}
  end


  def get_time_counter(result)
    result == 'OK' ? 1 : result.to_i
  end

  def lua_set_or_inc(seconds_to_expire)
    "return redis.call('set',KEYS[1], 1,'EX', #{seconds_to_expire}, 'NX') or redis.call('incr', KEYS[1])"
  end
end



# RollingWindow.new($redis_store_obj)