
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

  x.report("incr_windows_counter at random times (no expiring records - only redis incr)")  do
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
# incr_windows_counter      1.216k (± 2.5%) i/s -      6.102k in   5.022176s
# incr_windows_counter at random times (no expiring records - only redis incr)
#                           1.438k (± 1.7%) i/s -      7.191k in   5.002065s
# #query_since, one user, 5 minutes ago, all pmta records
#                           2.771k (± 3.3%) i/s -     13.932k in   5.032991s
# #query_since, one user, 1 hour ago, all pmta records
#                           2.430k (± 2.3%) i/s -     12.150k in   5.002994s
# #query_since, 24 hour ago, one user, only delivery records
#                           2.569k (± 2.6%) i/s -     12.844k in   5.002318s
# #query_since, 24 hour ago, one user, all pmta records
#                           1.841k (± 1.9%) i/s -      9.231k in   5.016388s
# #query_since, 24 hour ago, all users, all pmta records
#                           1.836k (± 1.7%) i/s -      9.250k in   5.040024s