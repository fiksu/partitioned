module Partitioned
  #
  # Partition tables by a time field grouping them by week, with
  # a week defined as seven days starting on Monday.
  #
  class ByMonthlyTimeField < ByTimeField
    self.abstract_class = true

    # Normalize a partition key value by month.
    #
    # @param [Time] time_value the time value to normalize
    # @return [Time] the value normalized
    def self.partition_normalize_key_value(time_value)
      return time_value.at_beginning_of_month
    end

    # The size of the partition table, a month
    # 
    # @return [Integer] the size of this partition
    def self.partition_table_size
      return 1.month
    end

    partitioned do |partition|
      partition.base_name lambda { |model, time_field|
        return model.partition_normalize_key_value(time_field).strftime('%Y%m')
      }
      
      partition.key_value lambda { |model, base_name|
        return DateTime.strptime(base_name, '%Y%m')
      }
    end
  end
end
