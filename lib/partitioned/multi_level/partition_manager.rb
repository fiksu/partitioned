module Partitioned
  class MultiLevel
    #
    # the manger of partitioned requests for models partitioned multiple times
    #
    class PartitionManager < Partitioned::PartitionedBase::PartitionManager
      #
      # Create new partitions at each partitioning level.
      def create_new_partitions(*partition_key_values)
        unless is_leaf_partition?(*partition_key_values)
          index = partition_key_values.length
          configurator.using_class(index).create_new_partitions(*partition_key_values)
          child_partition_names(*partition_key_values).each do |child_partition_name|
            create_new_partitions(*key_values_from_partition_name(child_partition_name))
          end
        end
      end

      #
      # Drop old partitions at each partitioning level.
      def drop_old_partitions(*partition_key_values)
        unless is_leaf_partition?(*partition_key_values)
          index = partition_key_values.length
          child_partition_names(*partition_key_values).each do |child_partition_name|
            drop_old_partitions(*key_values_from_partition_name(child_partition_name))
          end
          configurator.using_class(index).drop_old_partitions(*partition_key_values)
        end
      end

      #
      # Archive old partitions at each partitioning level.  No need to archive
      # non-leaf partitions: they contain no data.
      def archive_old_partitions(*partition_key_values)
        if is_leaf_partition?(*partition_key_values)
          index = partition_key_values.length
          configurator.using_class(index).archive_old_partitions(*partition_key_values)
        else
          child_partition_names(*partition_key_values).each do |child_partition_name|
            archive_old_partitions(*key_values_from_partition_name(child_partition_name))
          end
        end
      end
    end
  end
end
