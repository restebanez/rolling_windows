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
        { span: 1.second,   expiration: 1.hour, starts: :at_beginning_of_hour },
        { span: 10.seconds, expiration: 5.hour, starts: :at_beginning_of_hour },
        { span: 30.seconds, expiration: 1.day,  starts: :at_beginning_of_hour  },
        { span: 60.seconds, expiration: 1.day,  starts: :at_beginning_of_hour  },
        { span: 5.minutes, expiration: 1.day,   starts: :at_beginning_of_day  },
      ]
    }
    let(:rolling_window) { RollingWindow.new(redis, high_precision_time_windows) }


    describe '#incr_counter' do
      let(:user_id) { 1111 }
      subject { rolling_window.incr_windows_counter(record_type: 'd', user_id: user_id) }

      it 'creates as many redis keys as defined buckets' do
        expect(subject[:keys].size).to eq(high_precision_time_windows.size).and eq(redis.keys("at:*:for:*:#{user_id}:*").size)
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
