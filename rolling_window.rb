class RollingWindow
  attr_reader :redis

  def initialize(redis)
    @redis = redis
  end

  def register(user_id)
    time_precission = {current_second: Time.now.strftime('%S').to_i,
                       current_minute: Time.now.strftime('%M').to_i,
                       current_hour: Time.now.strftime('%H').to_i}

    time_precission[:current_user_second] = "user:#{user_id}:second:#{time_precission[:current_second]}"
    result = redis.eval(lua_set_or_inc(60), :keys => [time_precission[:current_user_second]])
    time_precission[:counter_second] = get_time_counter(result)

    time_precission[:current_user_minute] = "user:#{user_id}:minute:#{time_precission[:current_minute]}"
    result = redis.eval(lua_set_or_inc(60*60), :keys => [time_precission[:current_user_minute]])
    time_precission[:counter_minute] = get_time_counter(result)

    time_precission[:current_user_hour] = "user:#{user_id}:hour:#{time_precission[:current_hour]}"
    result = redis.eval(lua_set_or_inc(60*60*24), :keys => [time_precission[:current_user_hour]])
    time_precission[:counter_hour] = get_time_counter(result)

    time_precission
  end
end



# RollingWindow.new($redis_store_obj)