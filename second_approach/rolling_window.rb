require 'active_support'
require 'active_support/core_ext' # https://guides.rubyonrails.org/active_support_core_extensions.html

class RollingWindow
   TIME_WINDOWS = [ # each lenght is a factor of 6 of the previous one
     { span: 50.seconds, expiration: 25.hours, starts: :at_beginning_of_hour },
     { span: 5.minutes,  expiration: 36.hours, starts: :at_beginning_of_hour },
     { span: 30.minutes, expiration: 36.hours, starts: :at_beginning_of_hour },
     { span: 3.hours,    expiration: 48.hours, starts: :at_beginning_of_day },
     { span: 18.hours,   expiration: 7.days,   starts: :at_beginning_of_week },
     { span: 108.hours,  expiration: 30.days,  starts: :at_beginning_of_month }
   ]

  def initialize(redis, user_id)
    @redis = redis
    @user_id = user_id
  end

  def generate_bucket_names(epoch_since: , epoch_to: )
    epoch_since_time = Time.at(epoch_since)
    epoch_to_time = Time.at(epoch_to)
    #puts "count_max_number_of_buckets: #{count_max_number_of_buckets}"
    puts "Debug: epoch_since:#{epoch_since_time}"
    puts "Debug: epoch_to:   #{epoch_to_time}"
    puts "Debug: difference: #{epoch_to_time - epoch_since_time} seconds"
    #puts Time.at(epoch_since).at_beginning_of_day
    TIME_WINDOWS.reverse.each_with_object([]) do |bucket, list|
      # would it fit in this bucket without taking into account the starting time?
      if (epoch_to_time - epoch_since_time) > bucket[:span]
        puts "bucket #{bucket[:span]} fits"
        # does it fit within the starting time
        starts = epoch_since_time.send(bucket[:starts])
        ends = starts + bucket[:span]
        puts "First Window: starts: #{starts} - ends: #{ends}"
        # If start <= epoch_since_time; increase window by span
        while starts <= epoch_since_time do
          starts = starts + bucket[:span]
        end
        puts "Found Window: starts: #{starts} - ends: #{starts + bucket[:span]}"

      else
        puts "bucket #{bucket[:span]} NOT fit"
      end

    end
  end



   # This is just an informative method
  def count_max_number_of_buckets
    TIME_WINDOWS.inject(0) {|count, bucket| bucket[:expiration] / bucket[:span] + count  }
  end
end
