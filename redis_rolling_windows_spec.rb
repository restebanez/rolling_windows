require 'rubygems'
require 'bundler'

require 'redis'

$redis_store_obj = Redis.new

puts 'started'

def incr_time_precisions(user_id)
  time_precissions = {time_second: Time.now.strftime('%S').to_i,
                      time_minute: Time.now.strftime('%M').to_i,
                      time_hour: Time.now.strftime('%H').to_i}

  time_precissions[:time_second_key] = "user:#{user_id}:second:#{time_precissions[:time_second]}"
  result = $redis_store_obj.eval(lua_set_or_inc(60), :keys => [time_precissions[:time_second_key]])
  time_precissions[:counter_second] = get_time_counter(result)

  time_precissions[:time_minute_key] = "user:#{user_id}:minute:#{time_precissions[:time_minute]}"
  result = $redis_store_obj.eval(lua_set_or_inc(60*60), :keys => [time_precissions[:time_minute_key]])
  time_precissions[:counter_minute] = get_time_counter(result)

  time_precissions[:time_hour_key] = "user:#{user_id}:hour:#{time_precissions[:time_hour]}"
  result = $redis_store_obj.eval(lua_set_or_inc(60*60*24), :keys => [time_precissions[:time_hour_key]])
  time_precissions[:counter_hour] = get_time_counter(result)

  time_precissions
end

def get_time_counter(result)
  result == 'OK' ? 1 : result.to_i
end

def lua_set_or_inc(seconds_to_expire)
  "return redis.call('set',KEYS[1], 1,'EX', #{seconds_to_expire}, 'NX') or redis.call('incr', KEYS[1])"
end

def sum_last_x_seconds(user_id, last_seconds, current_second=Time.now.strftime('%S').to_i)
  seconds_range(end_second: current_second, seconds_back: last_seconds ).each_with_object({total: 0, time_second_keys: []}) do |second, hash|
    key = "user:#{user_id}:second:#{second}"
    value = $redis_store_obj.get(key).to_i
    puts "#{key} #{value}" if value > 0
    hash[:time_second_keys] << key if value > 0
    hash[:time_second_keys].uniq!
    hash[:total] += value
  end
end

def seconds_range(end_second:, seconds_back: )
  (0..seconds_back).map {|s| (end_second-=1) + 1 }.sort.map {|i| i < 0 ? i + 60 : i}
end

def get_last_seconds(start:, finish:)
  finish += 60 if finish < start
  finish - start
end

def send_stats_every_second_for_x_seconds_to_user(seconds, user_id)
  result = []
  result << incr_time_precisions(user_id)
  seconds.times{ |i| sleep 0.5;result << incr_time_precisions(user_id) }
  # we sum the last value of each :time_second_key
  total_sum = result.group_by {|h| h[:time_second]}.map {|a|a.last.last[:counter_second]}.sum
  time_second_keys = result.map{|h| h[:time_second_key]}
  [result.first[:time_second],result.last[:time_second],total_sum, time_second_keys.uniq]
end

def search_seconds_keys_redis(user_id)
  $redis_store_obj.keys("user:#{user_id}:second:*").reduce(0) do |sum, key|
    value = $redis_store_obj.get(key).to_i
    puts "#{key} #{value}"
    sum += value
  end
end

RSpec.describe "Rolling windows in Redis" do
  before do
    $redis_store_obj.flushall
    @user_id = 10360
    @start_second, @end_second, @total_count, @time_second_keys = send_stats_every_second_for_x_seconds_to_user(5, @user_id)
  end

  it "sums all the key's values of the last seconds" do
    puts "start: #{@start_second}, end: #{@end_second}"
    puts @time_second_keys.inspect
    total_sum_in_search_redis = search_seconds_keys_redis(@user_id)
    puts '------'
    last_seconds = get_last_seconds(start: @start_second, finish: @end_second)
    output = sum_last_x_seconds(@user_id, last_seconds, @end_second)
    expect(output[:total]).to eq(@total_count)
    expect(output[:total]).to eq(total_sum_in_search_redis)
    expect(output[:time_second_keys]).to eq(@time_second_keys)
  end
end
