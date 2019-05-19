require 'rubygems'
require 'bundler'
require 'time'

require 'redis'
require_relative 'rolling_window'

$redis_store_obj = Redis.new

RSpec.describe "Rolling windows in Redis" do
  let(:user_id) { 12345 }
  let(:rolling_window) { RollingWindow.new($redis_store_obj, user_id) }
  let(:epoch_since) { Time.iso8601("2019-05-18T18:01:29+01:00").to_i }

  context "computing one day one day" do
    let(:one_day_in_seconds) { 60 * 60 * 24}
    let(:epoch_to) { epoch_since + one_day_in_seconds  }

    it 'returns the buckes names within the time range' do
      bucket_names = rolling_window.get_bucket_names(epoch_since: epoch_since, epoch_to: epoch_to)
      expect(bucket_names).to eq('')
    end
  end
end