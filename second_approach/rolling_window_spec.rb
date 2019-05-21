require 'rubygems'
require 'bundler'
require 'time'

require 'redis'
require_relative 'rolling_window'

$redis_store_obj = Redis.new

RSpec.describe "Rolling windows in Redis" do
  let(:user_id) { 12345 }
  let(:rolling_window) { RollingWindow.new($redis_store_obj, user_id) }
  let(:epoch_since) { Time.iso8601("2019-05-18T18:02:29+01:00").to_i }

  context "computing one day one day" do
    let(:one_day_in_seconds) { 60 * 60 * 24}
    let(:epoch_to) { epoch_since + one_day_in_seconds  }

    it 'returns the buckes names within the time range' do
      bucket_names = rolling_window.generate_bucket_names(epoch_since: epoch_since, epoch_to: epoch_to)
      expect(bucket_names).to include(
        include(span: 1.minute,  window_starts: Time.parse('2019-05-18 18:03:00 +0100')),
        include(span: 1.minute,  window_starts: Time.parse('2019-05-18 18:04:00 +0100')),
        include(span: 5.minutes, window_starts: Time.parse('2019-05-18 18:05:00 +0100')),
        include(span: 5.minutes, window_starts: Time.parse('2019-05-18 18:10:00 +0100')))
    end
  end
end

=begin
+[{:span=>1 minute,
+  :window_finishes=>2019-05-18 18:04:00.000000000 +0100,
+  :window_starts=>2019-05-18 18:03:00.000000000 +0100},
+ {:span=>1 minute,
+  :window_finishes=>2019-05-18 18:05:00.000000000 +0100,
+  :window_starts=>2019-05-18 18:04:00.000000000 +0100},
+ {:span=>5 minutes,
+  :window_finishes=>2019-05-18 18:10:00.000000000 +0100,
+  :window_starts=>2019-05-18 18:05:00.000000000 +0100},
+ {:span=>5 minutes,
+  :window_finishes=>2019-05-18 18:15:00.000000000 +0100,
+  :window_starts=>2019-05-18 18:10:00.000000000 +0100},
+ {:span=>5 minutes,
+  :window_finishes=>2019-05-18 18:20:00.000000000 +0100,
+  :window_starts=>2019-05-18 18:15:00.000000000 +0100},
+ {:span=>5 minutes,
+  :window_finishes=>2019-05-18 18:25:00.000000000 +0100,
+  :window_starts=>2019-05-18 18:20:00.000000000 +0100},
+ {:span=>5 minutes,
+  :window_finishes=>2019-05-18 18:30:00.000000000 +0100,
+  :window_starts=>2019-05-18 18:25:00.000000000 +0100},
+ {:span=>30 minutes,
+  :window_finishes=>2019-05-18 19:00:00.000000000 +0100,
+  :window_starts=>2019-05-18 18:30:00.000000000 +0100},
+ {:span=>30 minutes,
+  :window_finishes=>2019-05-18 19:30:00.000000000 +0100,
+  :window_starts=>2019-05-18 19:00:00.000000000 +0100},
+ {:span=>30 minutes,
+  :window_finishes=>2019-05-18 20:00:00.000000000 +0100,
+  :window_starts=>2019-05-18 19:30:00.000000000 +0100},
+ {:span=>30 minutes,
+  :window_finishes=>2019-05-18 20:30:00.000000000 +0100,
+  :window_starts=>2019-05-18 20:00:00.000000000 +0100},
+ {:span=>30 minutes,
+  :window_finishes=>2019-05-18 21:00:00.000000000 +0100,
+  :window_starts=>2019-05-18 20:30:00.000000000 +0100},
+ {:span=>3 hours,
+  :window_finishes=>2019-05-19 00:00:00.000000000 +0100,
+  :window_starts=>2019-05-18 21:00:00.000000000 +0100},
+ {:span=>3 hours,
+  :window_finishes=>2019-05-19 03:00:00.000000000 +0100,
+  :window_starts=>2019-05-19 00:00:00.000000000 +0100},
+ {:span=>3 hours,
+  :window_finishes=>2019-05-19 06:00:00.000000000 +0100,
+  :window_starts=>2019-05-19 03:00:00.000000000 +0100},
+ {:span=>3 hours,
+  :window_finishes=>2019-05-19 09:00:00.000000000 +0100,
+  :window_starts=>2019-05-19 06:00:00.000000000 +0100},
+ {:span=>3 hours,
+  :window_finishes=>2019-05-19 12:00:00.000000000 +0100,
+  :window_starts=>2019-05-19 09:00:00.000000000 +0100},
+ {:span=>3 hours,
+  :window_finishes=>2019-05-19 15:00:00.000000000 +0100,
+  :window_starts=>2019-05-19 12:00:00.000000000 +0100},
+ {:span=>3 hours,
+  :window_finishes=>2019-05-19 18:00:00.000000000 +0100,
+  :window_starts=>2019-05-19 15:00:00.000000000 +0100},
+ {:span=>1 minute,
+  :window_finishes=>2019-05-19 18:01:00.000000000 +0100,
+  :window_starts=>2019-05-19 18:00:00.000000000 +0100},
+ {:span=>1 minute,
+  :window_finishes=>2019-05-19 18:02:00.000000000 +0100,
+  :window_starts=>2019-05-19 18:01:00.000000000 +0100}]
=end