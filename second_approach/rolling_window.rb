require 'active_support'
require 'active_support/core_ext' # https://guides.rubyonrails.org/active_support_core_extensions.html

class RollingWindow
   BUCKETS = [ # each lenght is a factor of 6 of the previous one
     { span: 50.seconds, expiration: 25.hours },
     { span: 5.minutes, expiration: 36.hours },
     { span: 30.minutes, expiration: 36.hours },
     { span: 3.hours, expiration: 48.hours },
     { span: 18.hours, expiration: 7.days },
   ]

  def initialize(redis, user_id)
    @redis = redis
    @user_id = user_id
  end

  def get_bucket_names(epoch_since: , epoch_to: )
    puts "count_max_number_of_buckets: #{count_max_number_of_buckets}"
    puts "Debug: epoch_since: #{epoch_since}. #{Time.at(epoch_since)}"
    puts "Debug: epoch_to:    #{epoch_to}. #{Time.at(epoch_to)}"
    puts "Debug: difference: #{epoch_to - epoch_since} seconds"
  end

   # This is just an informative method
  def count_max_number_of_buckets
    BUCKETS.inject(0) {|count, bucket| bucket[:expiration] / bucket[:span] + count  }
  end
end
