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
    search_finished_time_windows(epoch_since: epoch_since_time, epoch_to: epoch_to_time, found_windows: [])
    #puts Time.at(epoch_since).at_beginning_of_day
#    TIME_WINDOWS.reverse.each_with_object([]) do |bucket, list|
#      # would it fit in this bucket without taking into account the starting time?
#      if (epoch_to_time - epoch_since_time) > bucket[:span]
#        puts "bucket #{bucket[:span]} fits"
#        # does it fit within the starting time
#        starts = epoch_since_time.send(bucket[:starts])
#        ends = starts + bucket[:span]
#        puts "First Window: starts: #{starts} - ends: #{ends}"
#        # If start <= epoch_since_time; increase window by span
#        while starts <= epoch_since_time do
#          starts = starts + bucket[:span]
#        end
#        puts "Found Window: starts: #{starts} - ends: #{starts + bucket[:span]}"
#
#      else
#        puts "bucket #{bucket[:span]} NOT fit"
#      end
#
#    end
  end

  def search_finished_time_windows(epoch_since:, epoch_to:, found_windows: [], time_windows: TIME_WINDOWS.reverse)
    remaining_window = epoch_to - epoch_since
    puts "Recieve: diff: #{remaining_window}, epoch_since: #{epoch_since}, epoch_to: #{epoch_to}"
    if remaining_window < 2.minutes # change it to 1.minute
      puts "finish recursive"
      return found_windows
    else
      while bucket = time_windows.shift
        if remaining_window > bucket[:span]
          found_windows_in_this_time_span = []
          window_starts =  epoch_since.send(bucket[:starts])
          window_finishes = window_starts + bucket[:span]
          while window_finishes <= epoch_to do
            window_starts = window_starts + bucket[:span]
            window_finishes = window_starts + bucket[:span]

            if window_starts >= epoch_since and window_finishes <= epoch_to
              found_window = {window_starts: window_starts, window_finishes: window_finishes, span: bucket[:span]}
              puts "FOUND: #{found_window}"
              found_windows_in_this_time_span << found_window
            end
          end
          if found_windows_in_this_time_span.present?
            first_window_starts = found_windows_in_this_time_span.first[:window_starts]
            last_window_finishes = found_windows_in_this_time_span.last[:window_finishes]
            puts "first_window_starts: #{first_window_starts}, last_window_finishes: #{last_window_finishes}"
            #return found_windows + found_windows_in_this_time_span
            return search_finished_time_windows(epoch_since: epoch_since, epoch_to: first_window_starts, found_windows: found_windows + found_windows_in_this_time_span, time_windows: time_windows)
          end
        end
      end
    end

  end



   # This is just an informative method
  def count_max_number_of_buckets
    TIME_WINDOWS.inject(0) {|count, bucket| bucket[:expiration] / bucket[:span] + count  }
  end
end
