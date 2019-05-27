
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



# # Calculating -------------------------------------
# incr_windows_counter      1.223k (± 1.4%) i/s -      6.171k in   5.046634s
# incr_windows_counter at random times
#                           1.437k (± 1.7%) i/s -      7.200k in   5.011700s
# #query_since, one user, 5 minutes ago, all pmta records
#                           2.873k (± 3.1%) i/s -     14.504k in   5.054309s
# #query_since, one user, 1 hour ago, all pmta records
#                           2.687k (± 5.4%) i/s -     13.392k in   5.000834s
# #query_since, 24 hour ago, one user, only delivery records
#                           2.906k (± 2.7%) i/s -     14.612k in   5.031399s
# #query_since, 24 hour ago, one user, all pmta records
#                           2.066k (± 2.6%) i/s -     10.472k in   5.072816s
# #query_since, 24 hour ago, all users, all pmta records
#                           2.069k (± 2.0%) i/s -     10.422k in   5.040482s
#