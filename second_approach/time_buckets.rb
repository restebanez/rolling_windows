require 'active_support'
require 'active_support/core_ext' # https://guides.rubyonrails.org/active_support_core_extensions.html
# https://ruby-doc.org/core-2.6.3/Time.html
# https://ruby-doc.org/stdlib-2.6.3/libdoc/time/rdoc/Time.html
class TimeBuckets
  attr_reader :time_span_windows, :shorter_time_window_span

  DEFAULT_TIME_SPAN_WINDOWS = [
    { span: 1.minute,   expiration: 25.hours, starts: :at_beginning_of_hour },
    { span: 5.minutes,  expiration: 36.hours, starts: :at_beginning_of_hour },
    { span: 30.minutes, expiration: 36.hours, starts: :at_beginning_of_day  },
    { span: 3.hours,    expiration: 48.hours, starts: :at_beginning_of_day  },
    { span: 1.day,      expiration: 8.days,   starts: :last_week },
  ].freeze

  def initialize(time_span_windows_param = DEFAULT_TIME_SPAN_WINDOWS)
    # TODO validate:
    # there is at lest one window
    # each element has three keys
    @time_span_windows = time_span_windows_param.sort_by { |w| w[:span] }.reverse
    @shorter_time_window_span = time_span_windows.last[:span]
  end

  def find_time_buckets_in_range_sorted(*args)
    find_time_buckets_in_range(*args).sort_by { |w| w[:window_starts] }
  end

  # The search is different when current time (Since - from a definite past time until now) is used rather than arbitrary time_to
  # when using current time you can use unfinished window times
  def find_time_buckets_in_range(time_from:, time_to:, time_windows: time_span_windows)
    remaining_window = time_to - time_from
    puts "Recieve: diff: #{remaining_window}, time_from: #{time_from}, time_to: #{time_to}, time_window_left:  #{time_windows.size}"
    return [] if remaining_window < shorter_time_window_span

    while bucket = time_windows.shift
      puts "current time window to check: #{bucket[:span]}"
      next if remaining_window < bucket[:span]
      puts 'it may fit'
      found_windows = []
      window_starts = time_from.send(bucket[:starts])
      current_window = { window_starts: window_starts, window_finishes: window_starts + bucket[:span], span: bucket[:span] }

      while current_window[:window_finishes] <= time_to do
        if current_window[:window_starts] >= time_from and current_window[:window_finishes] <= time_to
          puts "FOUND: #{current_window}"
          found_windows << current_window.dup
        end
        current_window[:window_starts] += current_window[:span]
        current_window[:window_finishes] += current_window[:span]
      end

      next if found_windows.empty?

      first_window_starts = found_windows.first[:window_starts]
      last_window_finishes = found_windows.last[:window_finishes]
      puts "Found buckets range, first_window_starts: #{first_window_starts}, last_window_finishes: #{last_window_finishes}"
      return (found_windows +
          find_time_buckets_in_range(time_from: time_from,            time_to: first_window_starts, time_windows: time_windows.dup) +
          find_time_buckets_in_range(time_from: last_window_finishes, time_to: time_to,             time_windows: time_windows.dup))
    end
  end

  def stat_max_number_of_buckets
    time_span_windows.inject(0) {|count, bucket| bucket[:expiration] / bucket[:span] + count  }
  end
end
