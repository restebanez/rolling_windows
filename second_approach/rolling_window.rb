class RollingWindow
  def initialize(redis, user_id)
    @redis = redis
    @user_id = user_id
  end

  def get_bucket_names(epoch_since: , epoch_to: )
    puts "Debug: epoch_since: #{epoch_since}. #{Time.at(epoch_since)}"
    puts "Debug: epoch_to:    #{epoch_to}. #{Time.at(epoch_to)}"
    puts "Debug: difference: #{epoch_to - epoch_since} seconds"
  end
end
