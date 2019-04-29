require 'rubygems'
require 'bundler'

require 'redis'
require_relative 'rolling_window'



$redis_store_obj = Redis.new


def send_stats(user_id:, number_of_stats:, sleep_time: )
  result = []
  rolling_window = RollingWindow.new($redis_store_obj, user_id)
  result << rolling_window.register
  number_of_stats.times{ |i| sleep sleep_time; result << rolling_window.register }
  # we sum the last value of each :user_redis_key_per_second
  total_sum = result.group_by {|h| h[:current_second]}.map {|a|a.last.last[:counter_second]}.sum
  time_second_keys = result.map{|h| h[:user_redis_key_per_second]}
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
  context "Send stats every 0.2 seconds" do
    before do
      $redis_store_obj.flushall
      @user_id = 10360
      @start_second, @finish_second, @total_count, @redis_keys_used = send_stats(user_id: @user_id, number_of_stats: 13, sleep_time: 0.2 )
    end

    it "sums all the key's values of the last seconds" do
      puts "start: #{@start_second}, end: #{@finish_second}"
      total_sum_in_redis_search = search_seconds_keys_redis(@user_id)
      puts '------'
      output = RollingWindow.new($redis_store_obj, @user_id).sum_seconds_range(@start_second, @finish_second)
      expect(output[:total]).to eq(@total_count)
      expect(output[:total]).to eq(total_sum_in_redis_search)
      expect(output[:redis_keys_used]).to eq(@redis_keys_used)
    end
  end

  context "Send stats every 0.5 seconds" do
    before do
      $redis_store_obj.flushall
      @user_id = 10361
      @start_second, @finish_second, @total_count, @redis_keys_used = send_stats(user_id: @user_id, number_of_stats: 5, sleep_time: 0.5 )
    end

    it "sums all the key's values of the last seconds" do
      puts "start: #{@start_second}, end: #{@finish_second}"
      total_sum_in_redis_search = search_seconds_keys_redis(@user_id)
      puts '------'
      output = RollingWindow.new($redis_store_obj, @user_id).sum_seconds_range( @start_second, @finish_second)
      expect(output[:total]).to eq(@total_count)
      expect(output[:total]).to eq(total_sum_in_redis_search)
      expect(output[:redis_keys_used]).to eq(@redis_keys_used)
    end
  end

  context "Send stats every 2 seconds" do
    before do
      $redis_store_obj.flushall
      @user_id = 10362
      @start_second, @finish_second, @total_count, @redis_keys_used = send_stats(user_id: @user_id, number_of_stats: 5, sleep_time: 2 )
    end

    it "sums all the key's values of the last seconds" do
      puts "start: #{@start_second}, end: #{@finish_second}"
      total_sum_in_redis_search = search_seconds_keys_redis(@user_id)
      puts '------'
      output = RollingWindow.new($redis_store_obj, @user_id).sum_seconds_range(@start_second, @finish_second)
      expect(output[:total]).to eq(@total_count)
      expect(output[:total]).to eq(total_sum_in_redis_search)
      expect(output[:redis_keys_used]).to eq(@redis_keys_used)
    end
  end
end
