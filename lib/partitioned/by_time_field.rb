module Partitioned
  #
  # Partition tables by a time field grouping them by day.
  #
  class ByTimeField < PartitionedBase
    self.abstract_class = true

    #
    # Generate an enumerable that represents all the dates between
    # start_date and end_date skipping step.
    #
    # This can be used to calls that take an enumerable like create_infrastructure.
    #
    # @param [Date] start_date the first date to generate the range from
    # @param [Date] end_date the last date to generate the range from
    # @param [Object] step (:default) number of values to advance (:default means use {#self.partition_table_size}).
    # @return [Enumerable] the range generated
    def self.partition_generate_range(start_date, end_date, step = :default)
      step = partition_table_size if step == :default
      current_date = partition_normalize_key_value(start_date)
      dates = []
      while current_date <= end_date
        dates << current_date
        current_date += step
      end
      return dates
    end

    #
    # Normalize the value to the current day.
    #
    # @param [Time] time_value the partitioned key value
    # @return [Time] time_value normalized
    def self.partition_normalize_key_value(time_value)
      return time_value.to_date
    end

    #
    # The size of the partition, 1.day
    #
    # @return [Integer] the size of the partition
    def self.partition_table_size
      return 1.day
    end

    #
    # Abstract -- implement in a derived class.
    # The name of the time-related field we will use to partition child tables.
    #
    # @raise MethodNotImplemented
    def self.partition_time_field
      raise MethodNotImplemented.new(self, :partition_time_field)
    end

    partitioned do |partition|
      partition.on lambda {|model| model.partition_time_field}

      partition.index lambda {|model, *partition_key_values|
        return Configurator::Data::Index.new(model.partition_time_field, {})
      }

      partition.order :alphabetical, :direction => :ascending
      
      partition.key_value lambda { |model, base_name|
        return Date.strptime(base_name, '%Y%m%d')
      }

      partition.base_name lambda { |model, time_field|
        return model.partition_normalize_key_value(time_field).strftime('%Y%m%d')
      }
      partition.check_constraint lambda { |model, time_field|
        date = model.partition_normalize_key_value(time_field)
        return "#{model.partition_time_field} >= '#{date.strftime}' AND #{model.partition_time_field} < '#{(date + model.partition_table_size).strftime}'"
      }
    end
  end
end
