require 'rubygems'
require 'bundler'
require "rspec/json_expectations" # https://relishapp.com/waterlink/rspec-json-expectations/docs/json-expectations/array-matching-support-for-include-json-matcher

require 'time'

require_relative 'time_buckets'


RSpec.describe TimeBuckets do
  let(:time_buckets) { TimeBuckets.new }
  let(:epoch_since) { Time.parse("2019-05-18 18:02:29 +01:00") }

  context 'a few minutes' do
    let(:epoch_to) { epoch_since + 10.minutes  }
    subject { time_buckets.find_time_buckets_in_range_sorted(epoch_since: epoch_since, epoch_to: epoch_to) }

    it 'returns the largest possible time span windows within the time range' do
      puts JSON.pretty_generate(subject)
      expect(JSON.generate(subject)).to include_json(
        [
          {
            "window_starts": "2019-05-18 18:03:00 +0100",
            "window_finishes": "2019-05-18 18:04:00 +0100",
            "span": "60"
          },
          {
            "window_starts": "2019-05-18 18:04:00 +0100",
            "window_finishes": "2019-05-18 18:05:00 +0100",
            "span": "60"
          },
          {
            "window_starts": "2019-05-18 18:05:00 +0100",
            "window_finishes": "2019-05-18 18:10:00 +0100",
            "span": "300"
          },
          {
            "window_starts": "2019-05-18 18:10:00 +0100",
            "window_finishes": "2019-05-18 18:11:00 +0100",
            "span": "60"
          },
          {
            "window_starts": "2019-05-18 18:11:00 +0100",
            "window_finishes": "2019-05-18 18:12:00 +0100",
            "span": "60"
          }
        ]
      )
    end
  end

  context "computing one day" do
    let(:epoch_to) { epoch_since + 1.day  }

    subject { time_buckets.find_time_buckets_in_range_sorted(epoch_since: epoch_since, epoch_to: epoch_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(JSON.generate(subject)).to include_json(
        [
          {
            "window_starts": "2019-05-18 18:03:00 +0100",
            "window_finishes": "2019-05-18 18:04:00 +0100",
            "span": "60"
          },
          {
            "window_starts": "2019-05-18 18:04:00 +0100",
            "window_finishes": "2019-05-18 18:05:00 +0100",
            "span": "60"
          },
          {
            "window_starts": "2019-05-18 18:05:00 +0100",
            "window_finishes": "2019-05-18 18:10:00 +0100",
            "span": "300"
          },
          {
            "window_starts": "2019-05-18 18:10:00 +0100",
            "window_finishes": "2019-05-18 18:15:00 +0100",
            "span": "300"
          },
          {
            "window_starts": "2019-05-18 18:15:00 +0100",
            "window_finishes": "2019-05-18 18:20:00 +0100",
            "span": "300"
          },
          {
            "window_starts": "2019-05-18 18:20:00 +0100",
            "window_finishes": "2019-05-18 18:25:00 +0100",
            "span": "300"
          },
          {
            "window_starts": "2019-05-18 18:25:00 +0100",
            "window_finishes": "2019-05-18 18:30:00 +0100",
            "span": "300"
          },
          {
            "window_starts": "2019-05-18 18:30:00 +0100",
            "window_finishes": "2019-05-18 19:00:00 +0100",
            "span": "1800"
          },
          {
            "window_starts": "2019-05-18 19:00:00 +0100",
            "window_finishes": "2019-05-18 19:30:00 +0100",
            "span": "1800"
          },
          {
            "window_starts": "2019-05-18 19:30:00 +0100",
            "window_finishes": "2019-05-18 20:00:00 +0100",
            "span": "1800"
          },
          {
            "window_starts": "2019-05-18 20:00:00 +0100",
            "window_finishes": "2019-05-18 20:30:00 +0100",
            "span": "1800"
          },
          {
            "window_starts": "2019-05-18 20:30:00 +0100",
            "window_finishes": "2019-05-18 21:00:00 +0100",
            "span": "1800"
          },
          {
            "window_starts": "2019-05-18 21:00:00 +0100",
            "window_finishes": "2019-05-19 00:00:00 +0100",
            "span": "10800"
          },
          {
            "window_starts": "2019-05-19 00:00:00 +0100",
            "window_finishes": "2019-05-19 03:00:00 +0100",
            "span": "10800"
          },
          {
            "window_starts": "2019-05-19 03:00:00 +0100",
            "window_finishes": "2019-05-19 06:00:00 +0100",
            "span": "10800"
          },
          {
            "window_starts": "2019-05-19 06:00:00 +0100",
            "window_finishes": "2019-05-19 09:00:00 +0100",
            "span": "10800"
          },
          {
            "window_starts": "2019-05-19 09:00:00 +0100",
            "window_finishes": "2019-05-19 12:00:00 +0100",
            "span": "10800"
          },
          {
            "window_starts": "2019-05-19 12:00:00 +0100",
            "window_finishes": "2019-05-19 15:00:00 +0100",
            "span": "10800"
          },
          {
            "window_starts": "2019-05-19 15:00:00 +0100",
            "window_finishes": "2019-05-19 18:00:00 +0100",
            "span": "10800"
          },
          {
            "window_starts": "2019-05-19 18:00:00 +0100",
            "window_finishes": "2019-05-19 18:01:00 +0100",
            "span": "60"
          },
          {
            "window_starts": "2019-05-19 18:01:00 +0100",
            "window_finishes": "2019-05-19 18:02:00 +0100",
            "span": "60"
          }
        ]
      )
    end
  end
end
