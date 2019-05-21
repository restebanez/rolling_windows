require 'active_support'
require 'active_support/core_ext' # https://guides.rubyonrails.org/active_support_core_extensions.html
# https://ruby-doc.org/core-2.6.3/Time.html
# https://ruby-doc.org/stdlib-2.6.3/libdoc/time/rdoc/Time.html
class TimeBuckets
  attr_reader :time_span_windows

  DEFAULT_TIME_SPAN_WINDOWS = [
    { span: 1.minute,   expiration: 25.hours, starts: :at_beginning_of_hour },
    { span: 5.minutes,  expiration: 36.hours, starts: :at_beginning_of_hour },
    { span: 30.minutes, expiration: 36.hours, starts: :at_beginning_of_day  },
    { span: 3.hours,    expiration: 48.hours, starts: :at_beginning_of_day  },
    { span: 1.day,      expiration: 8.days,   starts: :last_week },
  ].freeze

  def initialize(time_span_windows = DEFAULT_TIME_SPAN_WINDOWS)
    @time_span_windows = time_span_windows.sort_by { |w| w[:span] }.reverse
  end
  
  def find_time_buckets_in_range_sorted(since: , to: )
    find_time_buckets_in_range(since: since, to: to).sort_by { |w| w[:window_starts] }
  end

  # The search is different when current time is used rather than arbitrary to
  # when using current time you can use unfinished window times
  def find_time_buckets_in_range(since:, to:, time_windows: time_span_windows)
    remaining_window = to - since
    puts "Recieve: diff: #{remaining_window}, since: #{since}, to: #{to}, time_window_left:  #{time_windows.size}"
    if remaining_window < time_span_windows.last[:span] #|| time_windows.blank?
      puts "finish recursive"
      return []
    else
      while bucket = time_windows.shift
        puts "current time window to check: #{bucket[:span]}"
        if remaining_window > bucket[:span]
          puts 'it may fit'
          found_windows = []
          window_starts =  since.send(bucket[:starts])
          window_finishes = window_starts + bucket[:span]
          while window_finishes <= to do
            if window_starts >= since and window_finishes <= to
              found_window = {window_starts: window_starts, window_finishes: window_finishes, span: bucket[:span]}
              puts "FOUND: #{found_window}"
              found_windows << found_window
            end
            window_starts = window_starts + bucket[:span]
            window_finishes = window_starts + bucket[:span]
          end
          if found_windows.present?
            first_window_starts = found_windows.first[:window_starts]
            last_window_finishes = found_windows.last[:window_finishes]

            puts "first_window_starts: #{first_window_starts}, last_window_finishes: #{last_window_finishes}"
            return (found_windows +
                find_time_buckets_in_range(since: since,          to: first_window_starts, time_windows: time_windows.dup) +
                find_time_buckets_in_range(since: last_window_finishes, to: to,            time_windows: time_windows.dup))
          end
        end
      end
      #[]
    end

  end

  def stat_max_number_of_buckets
    time_span_windows.inject(0) {|count, bucket| bucket[:expiration] / bucket[:span] + count  }
  end
end
