require 'rubygems'
require 'bundler'

require 'redis'
require_relative 'rolling_window'



$redis_store_obj = Redis.new


def send_stats_every_second_for_x_seconds_to_user(seconds, user_id)
  result = []
  rolling_window = RollingWindow.new($redis_store_obj)
  result << rolling_window.register(user_id)
  seconds.times{ |i| sleep 0.5;result << rolling_window.register(user_id) }
  # we sum the last value of each :current_user_second
  total_sum = result.group_by {|h| h[:current_second]}.map {|a|a.last.last[:counter_second]}.sum
  time_second_keys = result.map{|h| h[:current_user_second]}
  [result.first[:current_second],result.last[:current_second],total_sum, time_second_keys.uniq]
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
    @start_second, @finish_second, @total_count, @user_second_keys = send_stats_every_second_for_x_seconds_to_user(5, @user_id)
  end

  it "sums all the key's values of the last seconds" do
    puts "start: #{@start_second}, end: #{@finish_second}"
    puts @user_second_keys.inspect
    total_sum_in_redis_search = search_seconds_keys_redis(@user_id)
    puts '------'
    output = RollingWindow.new($redis_store_obj).sum_seconds_range(@user_id, @start_second, @finish_second)
    expect(output[:total]).to eq(@total_count)
    expect(output[:total]).to eq(total_sum_in_redis_search)
    expect(output[:user_second_keys]).to eq(@user_second_keys)
  end
end
