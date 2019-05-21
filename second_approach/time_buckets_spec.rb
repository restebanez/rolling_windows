require 'rubygems'
require 'bundler'
require "rspec/json_expectations" # https://relishapp.com/waterlink/rspec-json-expectations/docs/json-expectations/array-matching-support-for-include-json-matcher

require 'time'

require_relative 'time_buckets'


RSpec.describe TimeBuckets do
  let(:time_buckets) { TimeBuckets.new }

  context 'less than a minute' do
    let(:time_from) { Time.parse("2011-04-10 23:58:00 +01:00") }
    let(:time_to) { time_from + 30.seconds  }

    subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(subject).to be_empty
    end
  end

  context 'exactly 1 minute that starts in the 1 minute window' do
    let(:time_from) { Time.parse("2011-04-10 23:58:00 +01:00") }
    let(:time_to) { time_from + 1.minute  }

    subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(JSON.pretty_generate(subject)).to match_unordered_json(
        [
          {
            "window_starts": "2011-04-10 23:58:00 +0100",
            "window_finishes": "2011-04-10 23:59:00 +0100",
            "span": "60"
          }
        ]
      )
    end
  end

  context 'exactly a 5 minutes window' do
    let(:time_from) { Time.parse("2011-04-10 23:00:00 +01:00") }
    let(:time_to) { time_from + 5.minutes  }

    subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(JSON.pretty_generate(subject)).to match_unordered_json(
        [
          {
            "window_starts": "2011-04-10 23:00:00 +0100",
            "window_finishes": "2011-04-10 23:05:00 +0100",
            "span": "300"
          }
        ]
      )
    end
  end

  context 'exactly a 30 minutes window' do
    let(:time_from) { Time.parse("2011-04-10 23:00:00 +01:00") }
    let(:time_to) { time_from + 30.minutes  }

    subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(JSON.pretty_generate(subject)).to match_unordered_json(
        [
          {
            "window_starts": "2011-04-10 23:00:00 +0100",
            "window_finishes": "2011-04-10 23:30:00 +0100",
            "span": "1800"
          }
        ]
      )
    end
  end

  context 'exactly a 3 hours window' do
    let(:time_from) { Time.parse("2011-04-10 21:00:00 +01:00") }
    let(:time_to) { time_from + 3.hours  }

    subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(JSON.pretty_generate(subject)).to match_unordered_json(
        [
          {
            "window_starts": "2011-04-10 21:00:00 +0100",
            "window_finishes": "2011-04-11 00:00:00 +0100",
            "span": "10800"
          }
        ]
      )
    end
  end

  context 'exactly a 1 day window' do
    let(:time_from) { Time.parse("2011-04-10 00:00:00 +01:00") }
    let(:time_to) { time_from + 1.day  }

    subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(JSON.pretty_generate(subject)).to match_unordered_json(
        [
          {
            "window_starts": "2011-04-10 00:00:00 +0100",
            "window_finishes": "2011-04-11 00:00:00 +0100",
            "span": "86400"
          }
        ]
      )
    end
  end

  context 'maximum number of 1 minute buckets just before midnight' do
    let(:time_from) { Time.parse("2011-04-10 23:58:01 +01:00") }
    let(:time_to) { time_from + 6.minutes  }

    subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(JSON.pretty_generate(subject)).to match_unordered_json(
        [
          {
            "window_starts": "2011-04-10 23:59:00 +0100",
            "window_finishes": "2011-04-11 00:00:00 +0100",
            "span": "60"
          },
          {
            "window_starts": "2011-04-11 00:00:00 +0100",
            "window_finishes": "2011-04-11 00:01:00 +0100",
            "span": "60"
          },
          {
            "window_starts": "2011-04-11 00:01:00 +0100",
            "window_finishes": "2011-04-11 00:02:00 +0100",
            "span": "60"
          },
          {
            "window_starts": "2011-04-11 00:02:00 +0100",
            "window_finishes": "2011-04-11 00:03:00 +0100",
            "span": "60"
          },
          {
            "window_starts": "2011-04-11 00:03:00 +0100",
            "window_finishes": "2011-04-11 00:04:00 +0100",
            "span": "60"
          }
        ]
      )
    end
  end


  context 'a few minutes range at mid day' do
    let(:time_from) { Time.parse("2019-05-18 18:02:29 +01:00") }
    let(:time_to) { time_from + 10.minutes  }
    subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(JSON.generate(subject)).to match_unordered_json(
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
    let(:time_from) { Time.parse("2019-05-18 18:02:29 +01:00") }
    let(:time_to) { time_from + 1.day  }

    subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(JSON.generate(subject)).to match_unordered_json(
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

  context 'exactly a 7 day window' do
    let(:time_from) { Time.parse("2011-04-10 00:00:00 +01:00") }
    let(:time_to) { time_from + 7.day  }

    subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

    it 'returns the largest possible time span windows within the time range' do
      expect(JSON.pretty_generate(subject)).to match_unordered_json(
        [
          {
            "window_starts": "2011-04-10 00:00:00 +0100",
            "window_finishes": "2011-04-11 00:00:00 +0100",
            "span": "86400"
          },
          {
            "window_starts": "2011-04-11 00:00:00 +0100",
            "window_finishes": "2011-04-12 00:00:00 +0100",
            "span": "86400"
          },
          {
            "window_starts": "2011-04-12 00:00:00 +0100",
            "window_finishes": "2011-04-13 00:00:00 +0100",
            "span": "86400"
          },
          {
            "window_starts": "2011-04-13 00:00:00 +0100",
            "window_finishes": "2011-04-14 00:00:00 +0100",
            "span": "86400"
          },
          {
            "window_starts": "2011-04-14 00:00:00 +0100",
            "window_finishes": "2011-04-15 00:00:00 +0100",
            "span": "86400"
          },
          {
            "window_starts": "2011-04-15 00:00:00 +0100",
            "window_finishes": "2011-04-16 00:00:00 +0100",
            "span": "86400"
          },
          {
            "window_starts": "2011-04-16 00:00:00 +0100",
            "window_finishes": "2011-04-17 00:00:00 +0100",
            "span": "86400"
          }
        ]
      )
    end
  end

end
