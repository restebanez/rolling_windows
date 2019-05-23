require 'rubygems'
require 'bundler'
require "rspec/json_expectations"
require_relative 'time_buckets'

RSpec.describe TimeBuckets do
  context "using high precision span windows" do
    let(:high_precision_time_windows) {
      [
        { span: 1.second,   expiration: 1.hour },
        { span: 10.seconds, expiration: 5.hour },
        { span: 30.seconds, expiration: 1.day }
      ]
    }
    let(:time_buckets) { TimeBuckets.new(high_precision_time_windows) }

    context 'search since some time ago to now' do
      let(:time_since ) { Time.now - 31.seconds }
      subject { time_buckets.find_since_sorted(time_since: time_since) }

      it 'includes an unfinished open window' do
        expect(subject.last).to include(window_finishes: be > Time.now)
      end

      it 'includes at least one closed window' do
        expect(subject.first).to include(window_starts: be <= time_since + 1.second)
      end
    end

    context 'less than a minute' do
      let(:time_from) { Time.parse("2011-04-10 23:58:58 +01:00") }
      let(:time_to) { time_from + 43.seconds  }

      subject { time_buckets.find_in_range_sorted(time_from: time_from, time_to: time_to) }

      it 'returns the largest possible time span windows within the time range' do
        expect(JSON.pretty_generate(subject)).to match_unordered_json(
          [
            {
              "window_starts": "2011-04-10 23:58:58 +0100",
              "window_finishes": "2011-04-10 23:58:59 +0100",
              "span": "1"
            },
            {
              "window_starts": "2011-04-10 23:58:59 +0100",
              "window_finishes": "2011-04-10 23:59:00 +0100",
              "span": "1"
            },
            {
              "window_starts": "2011-04-10 23:59:00 +0100",
              "window_finishes": "2011-04-10 23:59:30 +0100",
              "span": "30"
            },
            {
              "window_starts": "2011-04-10 23:59:30 +0100",
              "window_finishes": "2011-04-10 23:59:40 +0100",
              "span": "10"
            },
            {
              "window_starts": "2011-04-10 23:59:40 +0100",
              "window_finishes": "2011-04-10 23:59:41 +0100",
              "span": "1"
            }
          ]
        )
      end
    end
  end

  context "using default time windows" do
    let(:default_time_windows) {
      [
        { span: 1.minute,   expiration: 25.hours },
        { span: 5.minutes,  expiration: 36.hours },
        { span: 30.minutes, expiration: 36.hours },
        { span: 3.hours,    expiration: 48.hours },
        { span: 1.day,      expiration: 8.days   },
      ]
    }

    let(:time_buckets) { TimeBuckets.new(default_time_windows) }

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
end
