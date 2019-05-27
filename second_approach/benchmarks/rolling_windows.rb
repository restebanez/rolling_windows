
require 'rubygems'
require 'bundler'
require 'benchmark/ips'

require 'redis'
require_relative '../rolling_window'
REDIS = Redis.new
REDIS.flushall
ROLLING_WINDOWS = RollingWindow.new(REDIS)
USER_ID = '11223'
ONE_DAY_AGO = 1.day.ago
TIME_NOW = Time.now

Benchmark.ips do |x|
  # Configure the number of seconds used during
  # the warmup phase (default 2) and calculation phase (default 5)
  x.config(:time => 5, :warmup => 2)
  x.report("incr_windows_counter") do
    ROLLING_WINDOWS.incr_windows_counter(pmta_record_type: 'd', user_id: USER_ID)
  end

  x.report("incr_windows_counter at random times")  do
    ROLLING_WINDOWS.incr_windows_counter(pmta_record_type: 'b', user_id: USER_ID, time: rand(ONE_DAY_AGO..TIME_NOW), never_expire: true)
  end

  x.report("#query_since, one user, 5 minutes ago, all pmta records") do
    ROLLING_WINDOWS.query_since(time_since: 5.minutes.ago , user_id: USER_ID)
  end

  x.report("#query_since, one user, 1 hour ago, all pmta records") do
    ROLLING_WINDOWS.query_since(time_since: 1.hour.ago , user_id: USER_ID)
  end

  x.report("#query_since, 24 hour ago, one user, only delivery records") do
    ROLLING_WINDOWS.query_since(time_since: 24.hour.ago , user_id: USER_ID, pmta_record_types: ['d'])
  end

  x.report("#query_since, 24 hour ago, one user, all pmta records") do
    ROLLING_WINDOWS.query_since(time_since: 24.hour.ago , user_id: USER_ID)
  end

  x.report("#query_since, 24 hour ago, all users, all pmta records") do
    ROLLING_WINDOWS.query_since(time_since: 24.hour.ago)
  end
end


# Calculating -------------------------------------
# incr_windows_counter      1.368k (± 1.4%) i/s -      6.888k in   5.036563s
# incr_windows_counter at random times
#                           1.330k (± 1.5%) i/s -      6.760k in   5.083068s
# #query_since, one user, 5 minutes ago, all pmta records
#                           3.430k (± 3.2%) i/s -     17.289k in   5.045780s
# #query_since, one user, 1 hour ago, all pmta records
#                           2.206k (± 2.4%) i/s -     11.220k in   5.089107s
# #query_since, 24 hour ago, one user, only delivery records
#                           3.141k (± 2.0%) i/s -     15.700k in   5.000960s
# #query_since, 24 hour ago, one user, all pmta records
#                           2.427k (± 2.5%) i/s -     12.291k in   5.067818s
# #query_since, 24 hour ago, all users, all pmta records
#                           2.427k (± 2.6%) i/s -     12.200k in   5.029395s