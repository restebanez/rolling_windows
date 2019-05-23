require 'rubygems'
require 'bundler'

require 'redis'
require_relative 'rolling_window'

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
      subject { rolling_window.incr_windows_counter(record_type: 'd', user_id: user_id) }

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
      subject { rolling_window.incr_windows_counter(record_type: 'd', user_id: user_id, time: time, adjust_expiration: false) }

      it 'creates as many redis keys as defined buckets' do
        expect(subject[:keys].size).to eq(high_precision_time_windows.size).and eq(redis.keys("at:*:for:*:#{user_id}:*").size)
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

    describe '#query_since' do
      context 'we collected a deliver 45 minutes ago ,half an hour ago, and just now' do
        let(:user_id) { 3333 }
        let(:time_since) { 45.minutes.ago }
        before do
          rolling_window.incr_windows_counter(record_type: 'd', user_id: user_id, time: time_since + 1.second, adjust_expiration: false)
          rolling_window.incr_windows_counter(record_type: 'd', user_id: user_id, time: 30.minutes.ago, adjust_expiration: false)
          rolling_window.incr_windows_counter(record_type: 'd', user_id: user_id, time: 30.minutes.ago, adjust_expiration: false)
          rolling_window.incr_windows_counter(record_type: 'd', user_id: user_id)
        end

        subject { rolling_window.query_since(time_since: time_since , user_id: user_id) }

        it 'sums all values' do
          expect(subject[:sum]).to eq(4)
        end

        it 'gets results from only three buckets' do
          expect(subject[:matched_queried_buckets_count]).to eq(3)
        end

        it 'required at least X number of windows' do
          elapsed_time = Time.now - time_since
          minimum_number_of_windows = elapsed_time / rolling_window.time_buckets.longest_time_window_span
          expect(subject[:queried_buckets_count]).to be > minimum_number_of_windows
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
