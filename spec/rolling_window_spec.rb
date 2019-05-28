require 'rubygems'
require 'bundler'

require 'redis'
require 'pp'
require_relative '../lib/rolling_window'

$redis_store_obj = Redis.new

def search_redis
  $redis_store_obj.keys("at:*").each do | key|
    value = $redis_store_obj.get(key)
    puts "key: #{key}  value: #{value}"
  end
end


RSpec.describe "Rolling windows in Redis" do
  let(:redis) {  Redis.new }
  before { redis.flushall }

  context "using high precision span windows" do
    let(:high_precision_time_windows) {
      [
        { span: 1.second,   expiration: 1.hour },
        { span: 10.seconds, expiration: 5.hour },
        { span: 30.seconds, expiration: 1.day  },
        { span: 60.seconds, expiration: 1.day  },
        { span: 5.minutes, expiration: 1.day   },
      ]
    }
    let(:rolling_window) { RollingWindow.new(redis, high_precision_time_windows) }


    describe '#incr_counter current time' do
      let(:user_id) { 1111 }
      subject { rolling_window.increment(pmta_record_type: 'd', user_id: user_id) }

      it 'creates as many redis keys as defined buckets' do
        expect(subject[:keys].size).to eq(high_precision_time_windows.size).and eq(redis.keys("at:*:for:*:#{user_id}:*").size)
      end

      it 'creates active open buckets using the current time' do
        expect(Time.now.floor_to(2)).to eq(subject[:creation_time].floor_to(2))
      end
    end

    describe '#incr_counter in the past' do
      let(:user_id) { 2222 }
      let(:time) { Time.parse("2011-04-10 13:00:03 +01:00") }
      subject { rolling_window.increment(pmta_record_type: 'd', user_id: user_id, time: time, never_expire: true) }

      it 'creates as many user redis keys as defined buckets' do
        expect(subject[:keys].size).to eq(high_precision_time_windows.size).and eq(redis.keys("at:*:for:*:#{user_id}:*").size)
      end

      it 'creates global all users stats' do
        expect(subject[:keys].size).to eq(redis.keys("at:*:for:*:all:*").size)
      end

      it 'creates active open buckets using the current time' do
        expect(time.floor_to(2)).to eq(subject[:creation_time].floor_to(2))
        expect(time.to_i).to be_within(subject[:windows][0][:window_starts].to_i).of(subject[:windows][0][:window_finishes].to_i)
        expect(time.to_i).to be_within(subject[:windows][1][:window_starts].to_i).of(subject[:windows][1][:window_finishes].to_i)
        expect(time.to_i).to be_within(subject[:windows][2][:window_starts].to_i).of(subject[:windows][2][:window_finishes].to_i)
        expect(time.to_i).to be_within(subject[:windows][3][:window_starts].to_i).of(subject[:windows][3][:window_finishes].to_i)
        expect(time.to_i).to be_within(subject[:windows][4][:window_starts].to_i).of(subject[:windows][4][:window_finishes].to_i)
      end
    end

    context 'increments at the very end of a window size' do
      let(:user_id) { 2223 }
      let(:at_the_end_of_hour) { Time.parse("2019-05-25 13:59:59 +01:00") }
      let(:at_the_begining_of_hour) { Time.parse("2019-05-25 14:00:00 +01:00") }
      before { rolling_window.increment(pmta_record_type: 'd', user_id: user_id, time: at_the_end_of_hour, never_expire: true) }
      subject { rolling_window.increment(pmta_record_type: 'd', user_id: user_id, time: at_the_begining_of_hour, never_expire: true) }

      it 'only writes to one window size at a time' do
        expect(subject[:keys]).to all( include(:value => 1) )
      end
    end

    describe '#query_since' do
      context 'we collected a deliver 45 minutes ago ,half an hour ago, and just now' do
        let(:user_id) { 3333 }
        let(:other_user_id) { 2342}
        let(:time_since) { 45.minutes.ago }
        before do
          rolling_window.increment(pmta_record_type: 'b', user_id: user_id, time: time_since + 1.second, never_expire: true)
          rolling_window.increment(pmta_record_type: 'd', user_id: user_id, time: 30.minutes.ago, never_expire: true)
          rolling_window.increment(pmta_record_type: 'd', user_id: user_id, time: 30.minutes.ago, never_expire: true)
          rolling_window.increment(pmta_record_type: 'd', user_id: other_user_id)
          rolling_window.increment(pmta_record_type: 'f', user_id: other_user_id)
          rolling_window.increment(pmta_record_type: 'b', user_id: user_id)
        end

        context 'query a user' do
          subject { rolling_window.query_since(time_since: time_since , user_id: user_id) }

          it 'sums all values' do
            expect(subject[:sum]).to eq(4)
          end

          it 'gets results from only three buckets' do
            expect(subject[:matched_queried_buckets_count]).to eq(3)
          end

          it 'reports stats per record type' do
            expect(subject[:stats_per_pmta_record_type]).to include(:b=>2, :d=>2)
          end

          it 'reports bounce rates' do
            expect(subject[:stats_per_pmta_record_type]).to include(:bounce_rate=>50)
          end

          it 'reports complaint rates' do
            expect(subject[:stats_per_pmta_record_type]).to include(:complaint_rate=>0)
          end

          it 'required at least X number of windows' do
            elapsed_time = Time.now - time_since
            minimum_number_of_windows = elapsed_time / rolling_window.time_buckets.longest_time_window_span
            expect(subject[:queried_buckets_count]).to be > minimum_number_of_windows
          end

          it 'perfoms in less than 1 milliseconds' do
            expect(subject[:redis_query_time]).to be < 1.0/1000
          end
        end

        context 'query a user by a specific type' do
          subject { rolling_window.query_since(time_since: time_since , user_id: user_id, pmta_record_types: ['d'] ) }

          it 'sums all values' do
            expect(subject[:sum]).to eq(2)
          end

          it 'reports stats per record type' do
            expect(subject[:stats_per_pmta_record_type]).to include(:d =>2)
          end

          it 'reports bounce rates' do
            expect(subject[:stats_per_pmta_record_type]).to include(:bounce_rate=>0)
          end

          it 'reports complaint rates' do
            expect(subject[:stats_per_pmta_record_type]).to include(:complaint_rate=>0)
          end
        end

        context 'query all users' do
          subject { rolling_window.query_since(time_since: time_since) }

          it 'sums all values' do
            expect(subject[:sum]).to eq(6)
          end

          it 'gets results from only three buckets' do
            expect(subject[:matched_queried_buckets_count]).to eq(5)
          end

          it 'reports stats per record type' do
            expect(subject[:stats_per_pmta_record_type]).to include(:b=>2, :d=>3, :f=>1)
          end

          it 'reports bounce rates' do
            expect(subject[:stats_per_pmta_record_type]).to include(:bounce_rate=>40.0)
          end

          it 'reports complaint rates' do
            expect(subject[:stats_per_pmta_record_type]).to include(:complaint_rate=>25.0)
          end
        end

      end

      context 'all the data expired some time ago' do
        let(:user_id) { 4444 }
        let(:expired_time) { 2.days.ago }
        before do
          # The never_expire allows you to write the record
          rolling_window.increment(pmta_record_type: 'b', user_id: user_id, time: expired_time + 1.second, never_expire: true)
          rolling_window.increment(pmta_record_type: 'd', user_id: user_id, time: expired_time + 15.minutes, never_expire: true)
          rolling_window.increment(pmta_record_type: 'd', user_id: user_id, time: expired_time + 30.minutes, never_expire: true)
        end

        context 'query a user' do
          subject { rolling_window.query_since(time_since: expired_time , user_id: user_id) }

          it 'sums all values' do
            expect(subject[:sum]).to eq(0)
          end

          it 'skips already expired buckets' do
            expect(subject[:skipped_expired_buckets].size).to be > 0
          end

          it 'queries non-expired buckets' do
            expect(subject[:queried_buckets].size).to be > 0
          end

          it 'queries non-expired buckets' do
            expect(subject[:matched_queried_buckets_count]).to eq(0)
          end
        end
      end
    end

  end
end

=begin
key: at:1558542260:for:10:u:2345:f  value: 1
key: at:1558542000:for:300:u:1111:f  value: 1
key: at:1558542240:for:60:u:1111:d  value: 2
key: at:1558542267:for:1:u:2345:f  value: 1
key: at:1558542240:for:60:u:2345:f  value: 1
key: at:1558542260:for:10:u:1111:f  value: 1
key: at:1558542267:for:1:u:1111:d  value: 2
key: at:1558542000:for:300:u:1111:d  value: 2
key: at:1558542240:for:30:u:1111:d  value: 2
key: at:1558542240:for:30:u:1111:f  value: 1
key: at:1558542240:for:30:u:2345:f  value: 1
key: at:1558542000:for:300:u:2345:f  value: 1
key: at:1558542267:for:1:u:1111:f  value: 1
key: at:1558542240:for:60:u:1111:f  value: 1
key: at:1558542260:for:10:u:1111:d  value: 2
=end
