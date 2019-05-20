require 'active_support'
require 'active_support/core_ext' # https://guides.rubyonrails.org/active_support_core_extensions.html
# https://ruby-doc.org/core-2.6.3/Time.html
# https://ruby-doc.org/stdlib-2.6.3/libdoc/time/rdoc/Time.html
class RollingWindow
  TIME_WINDOWS = [
     { span: 1.minute,   expiration: 25.hours, starts: :at_beginning_of_hour },
     { span: 5.minutes,  expiration: 36.hours, starts: :at_beginning_of_hour },
     { span: 30.minutes, expiration: 36.hours, starts: :at_beginning_of_day  },
     { span: 3.hours,    expiration: 48.hours, starts: :at_beginning_of_day  },
     { span: 1.day,      expiration: 8.days,   starts: :last_week },
   ].freeze

  def initialize(redis, user_id)
    puts "count_max_number_of_buckets: #{count_max_number_of_buckets}"
    @redis = redis
    @user_id = user_id
  end

  # The search is different when current time is used rather than arbitrary epoch_to
  # when using current time you can use unfinished window times
  def generate_bucket_names(epoch_since: , epoch_to: )
    epoch_since_time = Time.at(epoch_since)
    epoch_to_time = Time.at(epoch_to) # TODO, check if epoch_to_time is close to current time
    puts "Debug: epoch_since:#{epoch_since_time}"
    puts "Debug: epoch_to:   #{epoch_to_time}"
    puts "Debug: difference: #{epoch_to_time - epoch_since_time} seconds"
    search_finished_time_windows(epoch_since: epoch_since_time, epoch_to: epoch_to_time).sort_by { |w| w[:window_starts]}
  end

  def search_finished_time_windows(epoch_since:, epoch_to:, found_windows: [], time_windows: TIME_WINDOWS.reverse.deep_dup)
    remaining_window = epoch_to - epoch_since
    puts "Recieve: diff: #{remaining_window}, epoch_since: #{epoch_since}, epoch_to: #{epoch_to}, time_window_left:  #{time_windows.size}, found_windows: #{found_windows.size}"
    if remaining_window < 60 || time_windows.blank?
      puts "finish recursive"
      return []
    else
      while bucket = time_windows.shift
        puts "current time window to check: #{bucket[:span]}"
        if remaining_window > bucket[:span]
          puts 'it may fit'
          found_windows_in_this_time_span = []
          window_starts =  epoch_since.send(bucket[:starts])
          window_finishes = window_starts + bucket[:span]
          while window_finishes <= epoch_to do
            if window_starts >= epoch_since and window_finishes <= epoch_to
              found_window = {window_starts: window_starts, window_finishes: window_finishes, span: bucket[:span]}
              puts "FOUND: #{found_window}"
              found_windows_in_this_time_span << found_window
            end
            window_starts = window_starts + bucket[:span]
            window_finishes = window_starts + bucket[:span]
          end
          if found_windows_in_this_time_span.present?
            first_window_starts = found_windows_in_this_time_span.first[:window_starts]
            last_window_finishes = found_windows_in_this_time_span.last[:window_finishes]

            puts "first_window_starts: #{first_window_starts}, last_window_finishes: #{last_window_finishes}"
            return (found_windows_in_this_time_span +
                search_finished_time_windows(epoch_since: epoch_since,          epoch_to: first_window_starts, time_windows: time_windows.deep_dup) +
                search_finished_time_windows(epoch_since: last_window_finishes, epoch_to: epoch_to,            time_windows: time_windows.deep_dup))
          end
        end
      end
      []
    end

  end



   # This is just an informative method
  def count_max_number_of_buckets
    TIME_WINDOWS.inject(0) {|count, bucket| bucket[:expiration] / bucket[:span] + count  }
  end
end
